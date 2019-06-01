package zumount

import (
	"fmt"
	"log"
	"os/exec"
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
	// while mounts remain, try to unmount some more
	for len(mountsRemain) > 0 {
		// ns (namespace) is a pid
		ns, err := FindNamespaceToUnmount(dataset)
		if err != nil {
			return err
		}
		err = UnmountDatasetInNamespace(dataset, ns)
		if err != nil {
			log.Printf("failed unmounting %s in %s, but maybe made some progress, xontinuing... err: %s", dataset, ns, err)
		}
		mountsRemain, err = NamespacesForDataset(dataset)
		if err != nil {
			return err
		}
	}
	return nil
}

func NamespacesForDataset(dataset string) ([]string, err) {

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
				fullId,
				mountTable, err,
			)
			continue
		}
		// return the first namespace found, as we'll unmount
		// in there and then try again (recursively)
		for _, line := range strings.Split(string(mounts), "\n") {
			if strings.Contains(line, fullId) {
				shrapnel := strings.Split(mountTable, "/")
				// e.g. (0)/(1)proc/(2)X/(3)mounts
				pids = append(pids, shrapnel[2])
			}
		}
	}
	return pids, nil
}

func UnmountDatasetInNamespace(dataset, ns string) error {
	// this is a misnomer as it's not actually a zfs command...
	out, err := exec.Command(
		"nsenter", "-t", firstPidNSToUnmount, "-m", "-u", "-n", "-i",
		"umount", mountPath,
	).CombinedOutput()
	if rerr != nil {
		return &types.Event{
			Name: "failed-recovery-unmount",
			Args: &types.EventArgs{
				"original-err": err, "original-combined-output": string(out),
				"recovery-err": rerr, "recovery-combined-output": string(rout),
			},
		}, backoffState
	}
	// recurse, maybe we've made enough progress to be able to
	// mount this time?
	//
	// TODO limit recursion depth
	return f.mountSnap(snapId, readonly)
}
