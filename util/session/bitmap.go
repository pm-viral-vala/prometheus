package session

type bitmap struct {
	bits []bool //use uint64
}

func newBitMap(count int64) *bitmap {
	//return make([]uint64, 0, (count/(int64(sampleSize)*64))+1)
	return &bitmap{
		bits: make([]bool, count),
	}
}

func (b *bitmap) Get(id int) bool {
	if id < 0 || id > len(b.bits) {
		return false
	}
	return b.bits[id]
}
func (b *bitmap) Set(id int, value bool) {
	if !(id < 0 || id > len(b.bits)) {
		b.bits[id] = value
	}
}
