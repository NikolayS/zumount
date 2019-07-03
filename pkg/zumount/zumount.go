package zumount

import (
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
)

func AllDatasetsLeafFirst(pool string) ([]string, error) {
	out, err := exec.Command(
		"zfs", "list", "-o", "name", "-r", "-H", "-t", "snapshot,filesystem", pool,
	).CombinedOutput()
	if err != nil {
		if strings.Contains(err, "dataset does not exist") {
			// this is kindof a success, just say there are no such datasets
			// and we will succeed
			return []string{}, nil
		}
		return nil, fmt.Errorf("list gave %s, out: %s", err, out)
	}
	reversed := []string{}
	split := strings.Split(string(out), "\n")
	for i := len(split) - 1; i >= 0; i-- {
		line := split[i]
		if line != "" {
			reversed = append(reversed, line)
		}
	}
	return reversed, nil
}

func Clean(pool string) error {
	// 1. ask zfs all filesystems and snapshots of that pool, sort by length to do children first
	// 2. for each such dataset, find all mount namespaces with one or more such mounts (grep /proc/*/mounts)
	// 3. nsenter -t $pid -a and umount -f the places where that is mounted (grep output of mount)

	ds, err := AllDatasetsLeafFirst(pool)
	if err != nil {
		return err
	}
	for _, dataset := range ds {
		err := UnmountAll(dataset, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func UnmountAll(dataset string, try int) error {
	// dataset is a fully qualified zfs filesystem or snapshot name
	// e.g. pool/foo/bar/baz@snap
	namespacesMountpoints, err := AllNamespacesForDataset(dataset)
	if err != nil {
		return err
	}

	for namespace, mountpoints := range namespacesMountpoints {
		for _, mountpoint := range mountpoints {
			log.Printf(
				"> unmounting %s from mount ns %s (mounted at %s)...",
				dataset, namespace, mountpoint,
			)
			err = UnmountDatasetInNamespace(namespace, mountpoint)
			if err != nil {
				if strings.Contains(err.Error(), "not mounted") {
					// this happens a lot and is ok, don't log it
				} else {
					log.Printf(
						"failed unmounting %s in %s at %s, but maybe made some progress, continuing... err: %s",
						dataset, namespace, mountpoint, err,
					)
				}
			}
		}
	}
	// Check whether we succeeded - there should be no mounts anywhere now.
	mountsLeft, err := AllNamespacesForDataset(dataset)
	if err != nil {
		return err
	}
	if len(mountsLeft) > 0 {
		if try > 5 {
			return fmt.Errorf("Found %d mounts remaining after trying to remove them all, %d times, giving up!", len(mountsLeft), try)
		} else {
			log.Printf("Found %d mounts remaining, trying again (current try %d)", len(mountsLeft), try)
			return UnmountAll(dataset, try+1)
		}
	}
	return nil
}

// Return a list of mountpoints for the given dataset in all namespaces
func AllNamespacesForDataset(dataset string) (map[string][]string, error) {
	ret := map[string][]string{}
	mountTables, err := filepath.Glob("/proc/*/mounts")
	if err != nil {
		return nil, err
	}
	if mountTables == nil {
		return nil, fmt.Errorf("no mount tables in /proc/*/mounts")
	}
	for _, mountTable := range mountTables {
		mounts, err := ioutil.ReadFile(mountTable)
		if err != nil {
			// pids can disappear between globbing and reading
			continue
		}
		for _, line := range strings.Split(string(mounts), "\n") {
			if strings.Contains(line, dataset) {
				shrapnel := strings.Split(mountTable, "/")
				// e.g. (0)/(1)proc/(2)X/(3)mounts
				pid := shrapnel[2]
				lineShrapnel := strings.Split(line, " ")
				// e.g. 2a2c2a84-d91a-432c-bd4f-ac981e24f86a /var/lib/dotmesh/mnt/dmfs/83ec674c-8e5f-42cf-8527-97331bbf6163@2a2c2a84-d91a-432c-bd4f-ac981e24f86a zfs ro,noatime,xattr,noacl 0 0
				mountpoint := lineShrapnel[1]
				_, ok := ret[pid]
				if !ok {
					ret[pid] = []string{}
				}
				ret[pid] = append(ret[pid], mountpoint)
			}
		}
	}
	return ret, nil
}

func UnmountDatasetInNamespace(ns, mountpoint string) error {
	out, err := exec.Command(
		"nsenter", "-t", ns, "-a",
		"umount", mountpoint,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed nsenter umount of %s in ns %s, err: %s, out: %s", mountpoint, ns, err, out)
	}
	return nil
}
