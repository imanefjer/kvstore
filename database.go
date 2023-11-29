package main

type DB struct{
	wal *Wal
	tree *Tree
	sst *SStables
}
// Create a new database instance by initializing a new SSTable and Tree.
// Additionally, recover by reading values from the WAL
// in case of a crash during a previous connection, ensuring data integrity.
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
