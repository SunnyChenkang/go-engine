package synclist

import (
	"container/list"
	"sync"
)

type List struct {
	data *list.List
	lock sync.Mutex
}

func NewList() *List {
	q := new(List)
	q.data = list.New()
	return q
}

func (q *List) Push(v interface{}) {
	defer q.lock.Unlock()
	q.lock.Lock()
	q.data.PushFront(v)
}

func (q *List) Pop() interface{} {
	defer q.lock.Unlock()
	q.lock.Lock()
	iter := q.data.Back()
	v := iter.Value
	q.data.Remove(iter)
	return v
}

func (q *List) Range(f func(value interface{})) {
	defer q.lock.Unlock()
	q.lock.Lock()
	for iter := q.data.Back(); iter != nil; iter = iter.Prev() {
		f(iter.Value)
	}
}
