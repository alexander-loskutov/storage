package storage

import (
	"log"
	"os"
	"sync"
)

type StorageUpdater interface {
	start()
	onFileReceived(string)
}

func NewStorageUpdater(config *Configuration, consumer *FileConsumer, svc *PromotionsService) StorageUpdater {
	if config.Mode == SIMPLE {
		return newSimpleStorageUpdater(config, consumer, svc)
	}

	if config.Mode == IMMUTABLE {
		return newImmutableStorageUpdater(config, consumer, svc)
	}

	return nil
}

type BaseStorageUpdater struct {
	config   *Configuration
	consumer *FileConsumer
	svc      *PromotionsService

	onFileReceived func(string)
}

func newBaseStorageUpdater(config *Configuration, consumer *FileConsumer, svc *PromotionsService) *BaseStorageUpdater {
	return &BaseStorageUpdater{
		config:   config,
		consumer: consumer,
		svc:      svc,
	}
}

func (u *BaseStorageUpdater) start() {
	go u.consumer.WatchDirectory(resolvePath(u.config.InputDir))

	for file := range u.consumer.Files {
		u.onFileReceived(file)
	}
}

func (u *BaseStorageUpdater) openFile(filename string) (*os.File, error) {
	origFilename := filename
	filename += ".tmp"

	err := os.Rename(origFilename, filename)
	if err != nil {
		log.Printf("Failed to rename file %s. Error: %v\n", filename, err)
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Failed to open file %s. Error: %v\n", filename, err)
		return nil, err
	}

	return file, nil
}

func (u *BaseStorageUpdater) closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Printf("Failed to close file %s. Error: %v\n", file.Name(), err)
		return
	}

	err = os.Remove(file.Name())
	if err != nil {
		log.Printf("Failed to remove file %s. Error: %v\n", file.Name(), err)
	}
}

type SimpleStorageUpdater struct {
	*BaseStorageUpdater
}

func newSimpleStorageUpdater(
	config *Configuration,
	consumer *FileConsumer,
	svc *PromotionsService,
) *SimpleStorageUpdater {
	base := newBaseStorageUpdater(config, consumer, svc)
	u := &SimpleStorageUpdater{base}
	u.BaseStorageUpdater.onFileReceived = u.onFileReceived

	return u
}

func (u *SimpleStorageUpdater) onFileReceived(filename string) {
	file, err := u.openFile(filename)
	if err != nil {
		return
	}

	ch := make(chan *Promotion)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for p := range ch {
			u.svc.Save(p)
		}
	}()

	Process(file, PromotionFromCsv, ch)

	wg.Wait()
	u.closeFile(file)
}

type ImmutableStorageUpdater struct {
	*BaseStorageUpdater
}

func newImmutableStorageUpdater(
	config *Configuration,
	consumer *FileConsumer,
	svc *PromotionsService,
) *ImmutableStorageUpdater {
	base := newBaseStorageUpdater(config, consumer, svc)
	u := &ImmutableStorageUpdater{base}
	u.BaseStorageUpdater.onFileReceived = u.onFileReceived

	return u
}

func (u *ImmutableStorageUpdater) onFileReceived(filename string) {
	file, err := u.openFile(filename)
	if err != nil {
		return
	}

	data := make(chan *Promotion)
	go u.svc.Rewrite(data)

	ch := make(chan *Promotion)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for p := range ch {
			data <- p
		}
	}()

	Process(file, PromotionFromCsv, ch)
	wg.Wait()

	close(data)
	u.closeFile(file)
}
