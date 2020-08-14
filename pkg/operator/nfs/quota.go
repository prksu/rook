/*
Copyright 2020 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nfs

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"sigs.k8s.io/sig-storage-lib-external-provisioner/mount"
)

type Quota struct {
	shareProjectIDs map[string]map[uint16]bool

	mutex *sync.Mutex
}

func NewProjectQuota() (*Quota, error) {
	shareProjectIDs := map[string]map[uint16]bool{}
	mountEntries, err := findProjectQuotaMount()
	if err != nil {
		return nil, err
	}

	for _, entry := range mountEntries {
		shareDir := filepath.Base(entry.Mountpoint)
		shareProjectIDs[shareDir] = map[uint16]bool{}
		projectsFile := filepath.Join(entry.Mountpoint, "projects")
		_, err := os.Stat(projectsFile)
		if os.IsNotExist(err) {
			logger.Infof("creating new project file %s", projectsFile)
			file, cerr := os.Create(projectsFile)
			if cerr != nil {
				return nil, fmt.Errorf("error creating xfs projects file %s: %v", projectsFile, cerr)
			}

			file.Close()
		} else {
			logger.Infof("found project file %s, restoring project ids", projectsFile)
			re := regexp.MustCompile("(?m:^([0-9]+):/.+$)")
			projectIDs, err := restoreProjectIDs(projectsFile, re)
			if err != nil {
				logger.Errorf("error while populating projectIDs map, there may be errors setting quotas later if projectIDs are reused: %v", err)
			}

			shareProjectIDs[shareDir] = projectIDs
		}
	}

	projectQuota := &Quota{
		shareProjectIDs: shareProjectIDs,
		mutex:           &sync.Mutex{},
	}

	if err := projectQuota.RestoreQuotas(); err != nil {
		return nil, err
	}

	return projectQuota, nil
}

func (q *Quota) AddProject(projectFile, directory, bhard string) (uint16, string, error) {
	shareDir := filepath.Base(filepath.Dir(projectFile))
	projectID, block, err := addProjectID(q.mutex, q.shareProjectIDs[shareDir], projectFile, directory, bhard)
	if err != nil {
		return 0, "", fmt.Errorf("error adding project block %s to projects file %s: %v", block, projectFile, err)
	}

	projectIDStr := strconv.FormatUint(uint64(projectID), 10)
	logger.Info("running xfs_quota project command")
	cmd := exec.Command("xfs_quota", "-x", "-c", fmt.Sprintf("project -s -p %s %s", directory, projectIDStr), filepath.Dir(projectFile))
	out, err := cmd.CombinedOutput()
	if err != nil {
		_ = removeProjectID(q.mutex, projectID, q.shareProjectIDs[shareDir], projectFile, block)
		return 0, "", fmt.Errorf("xfs_quota failed with error: %v, output: %s", err, out)
	}

	logger.Info(string(out))
	return projectID, block, nil
}

func (q *Quota) RemoveProject(projectID uint16, projectFile, block string) error {
	shareDir := filepath.Base(filepath.Dir(projectFile))
	return removeProjectID(q.mutex, projectID, q.shareProjectIDs[shareDir], projectFile, block)
}

func (q *Quota) SetQuota(projectID uint16, projectFile, directory, bhard string) error {
	shareDir := filepath.Base(filepath.Dir(projectFile))

	if !q.shareProjectIDs[shareDir][projectID] {
		return fmt.Errorf("project with id %v has not been added", projectID)
	}

	projectIDStr := strconv.FormatUint(uint64(projectID), 10)
	cmd := exec.Command("xfs_quota", "-x", "-c", fmt.Sprintf("limit -p bhard=%s %s", bhard, projectIDStr), filepath.Dir(projectFile))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("xfs_quota failed with error: %v, output: %s", err, out)
	}

	return nil
}

func (q *Quota) RestoreQuotas() error {
	mountEntries, err := findProjectQuotaMount()
	if err != nil {
		return err
	}

	for _, entry := range mountEntries {
		projectsFile := filepath.Join(entry.Mountpoint, "projects")
		if _, err := os.Stat(projectsFile); err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return err
		}
		read, err := ioutil.ReadFile(projectsFile)
		if err != nil {
			return err
		}

		re := regexp.MustCompile("(?m:^([0-9]+):(.+):(.+)$\n)")
		matches := re.FindAllSubmatch(read, -1)
		for _, match := range matches {
			projectID, _ := strconv.ParseUint(string(match[1]), 10, 16)
			directory := string(match[2])
			bhard := string(match[3])

			if _, err := os.Stat(directory); os.IsNotExist(err) {
				_ = q.RemoveProject(uint16(projectID), projectsFile, string(match[0]))
				continue
			}

			_, _, _ = q.AddProject(projectsFile, directory, bhard)

			logger.Infof("restoring quotas from project file %s for project id %s", string(match[1]), projectsFile)
			if err := q.SetQuota(uint16(projectID), projectsFile, directory, bhard); err != nil {
				return fmt.Errorf("error restoring quota for directory %s: %v", directory, err)
			}
		}
	}

	return nil
}

func findProjectQuotaMount() ([]*mount.Info, error) {
	var entries []*mount.Info
	allEntries, err := mount.GetMounts()
	if err != nil {
		return nil, err
	}

	for _, entry := range allEntries {
		// we only support xfs
		if entry.Fstype != "xfs" {
			continue
		}

		if filepath.Dir(entry.Mountpoint) == mountPath && (strings.Contains(entry.VfsOpts, "pquota") || strings.Contains(entry.VfsOpts, "prjquota")) {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func initializeProjectQuota(shareProjectIDs map[string]map[uint16]bool, mountEntries []*mount.Info) error {
	for _, entry := range mountEntries {
		shareDir := filepath.Base(entry.Mountpoint)
		shareProjectIDs[shareDir] = map[uint16]bool{}
		projectsFile := filepath.Join(entry.Mountpoint, "projects")
		_, err := os.Stat(projectsFile)
		if os.IsNotExist(err) {
			file, cerr := os.Create(projectsFile)
			if cerr != nil {
				return fmt.Errorf("error creating xfs projects file %s: %v", projectsFile, cerr)
			}

			file.Close()
		} else {
			re := regexp.MustCompile("(?m:^([0-9]+):/.+$)")
			projectIDs, err := restoreProjectIDs(projectsFile, re)
			if err != nil {
				logger.Errorf("error while populating projectIDs map, there may be errors setting quotas later if projectIDs are reused: %v", err)
			}

			shareProjectIDs[shareDir] = projectIDs
		}
	}

	return nil
}

func addProjectID(mutex *sync.Mutex, ids map[uint16]bool, projectFile, directory, bhard string) (uint16, string, error) {
	mutex.Lock()
	id := uint16(1)
	for ; id < math.MaxUint16; id++ {
		if _, ok := ids[id]; !ok {
			break
		}
	}

	ids[id] = true

	block := strconv.FormatUint(uint64(id), 10) + ":" + directory + ":" + bhard + "\n"
	file, err := os.OpenFile(projectFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		mutex.Unlock()
		return 0, "", err
	}
	defer file.Close()

	if _, err = file.WriteString(block); err != nil {
		mutex.Unlock()
		return 0, "", err
	}

	if err := file.Sync(); err != nil {
		mutex.Unlock()
		return 0, "", err
	}

	mutex.Unlock()
	return id, block, nil
}

func removeProjectID(mutex *sync.Mutex, id uint16, ids map[uint16]bool, path, block string) error {
	mutex.Lock()
	delete(ids, id)
	read, err := ioutil.ReadFile(path)
	if err != nil {
		mutex.Unlock()
		return err
	}

	removed := strings.Replace(string(read), block, "", -1)
	err = ioutil.WriteFile(path, []byte(removed), 0)
	if err != nil {
		mutex.Unlock()
		return err
	}

	mutex.Unlock()
	return nil
}

func restoreProjectIDs(projectFile string, re *regexp.Regexp) (map[uint16]bool, error) {
	ids := map[uint16]bool{}
	digitsRe := "([0-9]+)"
	if !strings.Contains(re.String(), digitsRe) {
		return ids, fmt.Errorf("regexp %s doesn't contain digits submatch %s", re.String(), digitsRe)
	}

	read, err := ioutil.ReadFile(projectFile)
	if err != nil {
		return ids, err
	}

	allMatches := re.FindAllSubmatch(read, -1)
	for _, match := range allMatches {
		digits := match[1]
		if id, err := strconv.ParseUint(string(digits), 10, 16); err == nil {
			ids[uint16(id)] = true
		}
	}

	return ids, nil
}
