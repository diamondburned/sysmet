package sysmet

// This file only contains functions that dereference the return values.

import (
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

func diskUsages() ([]disk.UsageStat, error) {
	pstat, err := disk.Partitions(false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get all partitions")
	}

	ustat := make([]disk.UsageStat, 0, len(pstat))
	for _, p := range pstat {
		u, err := disk.Usage(p.Mountpoint)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get usage for mount %q", p.Mountpoint)
		}
		ustat = append(ustat, *u)
	}

	return ustat, nil
}

func virtualMemory() (mem.VirtualMemoryStat, error) {
	m, err := mem.VirtualMemory()
	if err != nil {
		return mem.VirtualMemoryStat{}, nil
	}
	return *m, nil
}

func swapMemory() (mem.SwapMemoryStat, error) {
	m, err := mem.SwapMemory()
	if err != nil {
		return mem.SwapMemoryStat{}, nil
	}
	return *m, nil
}

func loadAvg() (load.AvgStat, error) {
	l, err := load.Avg()
	if err != nil {
		return load.AvgStat{}, nil
	}
	return *l, nil
}

func loadMisc() (load.MiscStat, error) {
	l, err := load.Misc()
	if err != nil {
		return load.MiscStat{}, nil
	}
	return *l, nil
}
