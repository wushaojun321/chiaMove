package main

import (
	"errors"
	"fmt"
	"github.com/thoas/go-funk"
	"golang.org/x/sys/unix"
	yaml "gopkg.in/yaml.v2"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

type Config struct {
	FromPaths      []string `yaml:"fromPaths"`
	ToPaths        []string `yaml:"toPaths"`
	FromPathFilter struct {
		MinSize uint64 `yaml:"minSize"`
		MaxSize uint64 `yaml:"maxSize"`
		Prefix  string `yaml:"prefix"`
	} `yaml:"fromPathFilter"`
}

var config *Config
var invalidPath []string

func ReadConfig(filename string) (*Config, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(buf, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func GetRemindSizeByPath(path string) (uint64, error) {
	fs := unix.Statfs_t{}
	err := unix.Statfs(path, &fs)
	if err != nil {
		fmt.Printf("Error getting filesystem info: %s\n", err)
		return 0, err
	}
	freeSpace := fs.Bavail * uint64(fs.Bsize)
	return freeSpace, nil
}

type Executor struct {
	fromPath string
	toPath   string
}

var (
	wg sync.WaitGroup
	mu sync.Mutex
)

func getDirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	return size, err
}

func getCanMovePath(fromPath string) (string, error) {
	entries, err := os.ReadDir(fromPath)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		filename := entry.Name()
		relativePath := filepath.Join(fromPath, entry.Name())
		if entry.IsDir() && strings.HasPrefix(filename, config.FromPathFilter.Prefix) {
			size, err := getDirSize(relativePath)
			if err != nil {
				fmt.Printf("获取路径 %s 的大小失败 %v\n", relativePath, err)
				panic("")
			}
			if config.FromPathFilter.MinSize <= size && size < config.FromPathFilter.MaxSize {
				return relativePath, nil
			}
		}
	}
	return "", errors.New("未获取到符合条件的文件夹")
}

func CopySourceToDestination(src, dst string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return fmt.Errorf("源目录不存在: %w", err)
	}
	// 使用rsync命令进行复制，支持断点续传
	// --partial 使得rsync在单个文件传输被中断时保留部分文件，以便续传
	// --append 使用文件已传输的部分，无需重新传输
	cmd := exec.Command("rsync", "-avz", "--partial", "--append", "--remove-source-files", src, dst)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("rsync命令执行出错: %w", err)
	}
	if err := os.RemoveAll(src); err != nil {
		return fmt.Errorf("删除源目录出错: %w", err)
	}
	return nil
}

func afterHook() {
	if len(invalidPath) > 0 {
		fmt.Println("有问题的文件夹如下：")
		for _, path := range invalidPath {
			fmt.Println(path)
		}
	}
}

func main() {
	var err error
	config, err = ReadConfig("config.yaml")
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}
	for {
		var executors []*Executor
		for _, fromPath := range config.FromPaths {
			fromChildPath, err := getCanMovePath(fromPath)
			if err != nil {
				continue
			}
			if !funk.Contains(invalidPath, fromChildPath) {
				executors = append(executors, &Executor{fromPath: fromChildPath})
			}
		}
		if len(executors) == 0 {
			fmt.Println("A盘已空，请换盘！")
			afterHook()
			return
		}
		index := 0
		for _, toPath := range config.ToPaths {
			if index >= len(executors) {
				break
			}
			size, _ := GetRemindSizeByPath(toPath)
			if size > config.FromPathFilter.MaxSize {
				executors[index].toPath = toPath
				index += 1
			}
		}
		if index == 0 {
			fmt.Println("B盘已满，任务完成！")
			afterHook()
			return
		}
		for _, exe := range executors {
			wg.Add(1)
			go func(exe *Executor) {
				defer wg.Done()
				fmt.Printf("%s -> %s 开始...\n", exe.fromPath, exe.toPath)
				err := CopySourceToDestination(exe.fromPath, exe.toPath)
				if err != nil {
					fmt.Printf("%s -> %s 复制失败 %v\n", exe.fromPath, exe.toPath, err)
					mu.Lock()
					invalidPath = append(invalidPath, exe.fromPath)
					mu.Unlock()
				} else {
					fmt.Printf("%s -> %s 复制成功\n", exe.fromPath, exe.toPath)
				}
			}(exe)
		}
		wg.Wait()
	}
}
