package graph

// labelQueue is a min-heap of search labels, by fee then hop count,
// implementing heap.Interface.
type labelQueue struct {
	items []*label
}

func (q *labelQueue) Len() int { return len(q.items) }

func (q *labelQueue) Less(i, j int) bool {
	a, b := q.items[i], q.items[j]
	if a.fee != b.fee {
		return a.fee < b.fee
	}
	return a.hops < b.hops
}

func (q *labelQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

func (q *labelQueue) Push(x any) { q.items = append(q.items, x.(*label)) }

func (q *labelQueue) Pop() any {
	n := len(q.items)
	item := q.items[n-1]
	q.items[n-1] = nil // avoid memory leak
	q.items = q.items[:n-1]
	return item
}
