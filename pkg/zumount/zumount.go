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
	mountsRemain, err := NamespacesForDataset(dataset)
	if err != nil {
		return err
	}
	var lastMountsRemain int
	// while mounts remain, try to unmount some more
	for len(mountsRemain) > 0 {
		for _, ns := range mountsRemain {
			err = UnmountDatasetInNamespace(dataset, ns)
			if err != nil {
				log.Printf("failed unmounting %s in %s, but maybe made some progress, xontinuing... err: %s", dataset, ns, err)
			}
			lastMountsRemain = len(mountsRemain)
			// XXX does updating mountsRemain while iterating over it make sense?
			mountsRemain, err = NamespacesForDataset(dataset)
			if err != nil {
				return err
			}
			if lastMountsRemain == len(mountsRemain) {
				return fmt.Errorf("made no progress, aborting")
			}
			if len(mountsRemain) == 0 {
				break
			}
		}
	}
	return nil
}

func NamespacesForDataset(dataset string) ([]string, error) {

	mountTables, err := filepath.Glob("/proc/*/mounts")
	if err != nil {
		return nil, err
	}
	if mountTables == nil {
		return nil, fmt.Errorf("no mount tables in /proc/*/mounts")
	}
	pids := []string{}
	for _, mountTable := range mountTables {
		mounts, err := ioutil.ReadFile(mountTable)
		if err != nil {
			// pids can disappear between globbing and reading
			log.Printf(
				"[mount:%s] ignoring error reading pid mount table %v: %v",
				dataset,
				mountTable, err,
			)
			continue
		}
		for _, line := range strings.Split(string(mounts), "\n") {
			if strings.Contains(line, dataset) {
				shrapnel := strings.Split(mountTable, "/")
				// e.g. (0)/(1)proc/(2)X/(3)mounts
				pids = append(pids, shrapnel[2])
			}
		}
	}
	return pids, nil
}

func UnmountDatasetInNamespace(dataset, ns string) error {
	out, err := exec.Command(
		"nsenter", "-t", ns, "-a",
		"umount", "--all-targets", dataset,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed nsenter umount of %s in ns %s, err: %s, out: %s", dataset, ns, err, out)
	}
	return nil
}
