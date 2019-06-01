package zumount

func Clean(pool string) error {
	// 1. ask zfs all filesystems and snapshots of that pool, sort by length to do children first
	// 2. for each such dataset, find all mount namespaces with one or more such mounts (grep /proc/*/mounts)
	// 3. nsenter -t $pid -a and umount -f the places where that is mounted (grep output of mount)

	for _, dataset := range AllDatasetsLeafFirst(pool) {
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
	mountsRemain := DoMountsRemain(dataset)
	// while mounts remain, try to unmount the next one
	for mountsRemain {
		// ns (namespace) is a pid
		ns, err := FindNamespaceToUnmount(dataset)
		if err != nil {
			return err
		}
		err = UnmountDatasetInNamespace(dataset, ns)
		if err != nil {
			return err
		}
		mountsRemain = DoMountsRemain(dataset)
	}
	return nil
}

func ReusableCode() {

	out, err := f.zfs.Mount(f.filesystemId, snapId, options, mountPath)
	if err != nil {
		if strings.Contains(string(out), "already mounted") {
			// This can happen when the filesystem is mounted in some other
			// namespace for some reason. Try searching for it in all
			// processes' mount namespaces, and recursively unmounting it
			// from one namespace at a time until becomes free...
			firstPidNSToUnmount, rerr := func() (string, error) {
				mountTables, err := filepath.Glob("/proc/*/mounts")
				if err != nil {
					return "", err
				}
				if mountTables == nil {
					return "", fmt.Errorf("no mount tables in /proc/*/mounts")
				}
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
							return shrapnel[2], nil
						}
					}
				}
				return "", fmt.Errorf("unable to find %s in any /proc/*/mounts", fullId)
			}()
			if rerr != nil {
				return &types.Event{
					Name: "failed-finding-namespace-to-unmount",
					Args: &types.EventArgs{
						"original-err": err, "original-combined-output": string(out),
						"recovery-err": rerr,
					},
				}, backoffState
			}
			log.Printf(
				"[mount:%s] attempting recovery-unmount in ns %s after %v/%v",
				fullId, firstPidNSToUnmount, err, string(out),
			)
			// this is a misnomer as it's not actually a zfs command...
			rout, rerr := exec.Command(
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
	}
}
