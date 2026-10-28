package reload

import "github.com/fsnotify/fsnotify"

type eventSource interface {
	Watch(dirs []string)
	Events() <-chan fsnotify.Event
	Errors() <-chan error
	Close() error
}

type fsEvents struct {
	w    *fsnotify.Watcher
	dirs map[string]bool
}

func newFSEvents() (eventSource, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &fsEvents{w: w, dirs: map[string]bool{}}, nil
}

func (f *fsEvents) Watch(dirs []string) {
	want := make(map[string]bool, len(dirs))
	for _, d := range dirs {
		want[d] = true
		if f.dirs[d] {
			continue
		}
		if err := f.w.Add(d); err != nil {
			continue
		}
		f.dirs[d] = true
	}
	for d := range f.dirs {
		if !want[d] {
			_ = f.w.Remove(d)
			delete(f.dirs, d)
		}
	}
}

func (f *fsEvents) Events() <-chan fsnotify.Event {
	return f.w.Events
}

func (f *fsEvents) Errors() <-chan error {
	return f.w.Errors
}

func (f *fsEvents) Close() error {
	return f.w.Close()
}
