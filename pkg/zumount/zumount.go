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
		err := UnmountAll(dataset)
		if err != nil {
			return err
		}
	}
	return nil
}

func UnmountAll(dataset string) error {
	// dataset is a fully qualified zfs filesystem or snapshot name
	// e.g. pool/foo/bar/baz@snap
	nextNS, mountpoint, err := OneNamespaceForDataset(dataset)
	if err != nil {
		return err
	}
	var lastNS string
	// while mounts remain, try to unmount some more
	for nextNS != "" {
		log.Printf("> unmounting %s from mount ns %s...", dataset, nextNS)
		err = UnmountDatasetInNamespace(nextNS, mountpoint)
		if err != nil {
			log.Printf(
				"failed unmounting %s in %s, but maybe made some progress, continuing... err: %s",
				dataset, nextNS, err,
			)
		}
		lastNS = nextNS
		nextNS, mountpoint, err = OneNamespaceForDataset(dataset)
		if err != nil {
			return err
		}
		if lastNS == nextNS {
			return fmt.Errorf("made no progress (ns %s got stuck), aborting", lastNS)
		}
	}
	return nil
}

func OneNamespaceForDataset(dataset string) (string, string, error) {
	mountTables, err := filepath.Glob("/proc/*/mounts")
	if err != nil {
		return "", "", err
	}
	if mountTables == nil {
		return "", "", fmt.Errorf("no mount tables in /proc/*/mounts")
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
				return pid, mountpoint, nil
			}
		}
	}
	return "", "", nil
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
