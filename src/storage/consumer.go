package storage

import (
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

type FileConsumer struct {
	Files chan string

	mutex  *sync.Mutex
	timers map[string]*time.Timer
}

func NewFileConsumer() *FileConsumer {
	return &FileConsumer{
		Files:  make(chan string),
		mutex:  &sync.Mutex{},
		timers: make(map[string]*time.Timer),
	}
}

func (f *FileConsumer) WatchDirectory(dir string) {
	_ = os.Mkdir(dir, os.ModePerm)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Failed to create file system watcher: %v", err)
	}
	defer w.Close()

	go f.watch(w)

	err = w.Add(dir)
	if err != nil {
		log.Fatalf("Failed to add directory %s to watcher: %v", dir, err)
	}
	log.Println("Watching directory " + dir)

	forever := make(chan bool)
	<-forever
}

func (f *FileConsumer) watch(w *fsnotify.Watcher) {
	for {
		select {
		case err, ok := <-w.Errors:
			if !ok {
				return
			}

			log.Printf("Error while watching file system: %v\n", err)
		case e, ok := <-w.Events:
			if !ok {
				return
			}

			if !strings.HasSuffix(e.Name, ".csv") {
				continue
			}

			if !e.Has(fsnotify.Create) && !e.Has(fsnotify.Write) {
				continue
			}

			f.mutex.Lock()
			timer, ok := f.timers[e.Name]
			f.mutex.Unlock()

			if !ok {
				fn := func() {
					f.Files <- e.Name
					log.Println("Received file " + e.Name)
				}

				// Create a timer to wait for the WRITE events to stop coming in
				// After that we consider the file is created and ready to be processed
				timer = time.AfterFunc(math.MaxInt64, fn)
				timer.Stop()

				f.mutex.Lock()
				f.timers[e.Name] = timer
				f.mutex.Unlock()
			}

			timer.Reset(100 * time.Millisecond)
		}
	}
}
