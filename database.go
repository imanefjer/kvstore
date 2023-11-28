package main

type DB struct{
	wal *Wal
	tree *Tree
	sst *SStables
}

func NewDB(wal *Wal) (*DB, error){
	tree := Tree{}
	err := Recover(wal, &tree)
	if err != nil {
		return nil,err
	}
	sst, err := NewSST("sstFiles")
	if err != nil {
		return nil, err
	}
	return &DB{
		wal: wal,
		tree: &tree,
		sst: sst,
	},nil
}
