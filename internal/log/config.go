package log

type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		maxIndexBytes uint64
		InitialOffset uint64
	}
}
