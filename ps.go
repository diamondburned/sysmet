package sysmet

// This file only contains functions that dereference the return values.

import (
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

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
