package csvrecord

import (
	"fmt"
	"testing"
)

//tag 标签中的名字与csv表中相同 该字段需要作为索引字段时请在 tag 中添加 index:trues
type Address struct {
	Id     int     `csv:"id" index:"true"`
	Name   string  `csv:"name" index:"true"`
	Email  string  `csv:"email"`
	Height float32 `csv:"height"`
	Keys   []int   `csv:"keys"`
	Kks    [][]int `csv:"kks"`
}

func (self *Address) ToString() string {
	return fmt.Sprintf("[%d] [%s] [%s] [%f]", self.Id, self.Name, self.Email, self.Height)
}

func TestRead(t *testing.T) {
	cr, err := New(Address{})
	if err != nil {
		t.Fatal("New error ", err)
	}

	err = cr.Read("test.csv")
	if err != nil {
		t.Fatal("Read error ", err)
	}

	if len(cr.indexesMap) != 2 {
		t.Fatal("indexesMap size != 2")
	}

	if len(cr.records) != 3 {
		t.Fatal("records != 3")
	}

	for i := 0; i < cr.NumRecrod(); i++ {
		t.Log(cr.Record(i))
	}

	for i := 1001; i <= 1004; i++ {
		t.Log(i, " : ", cr.Index("id", i))
	}

	t.Log("John Done : ", cr.Index("name", "John Done"))
	t.Log("test1 : ", cr.Index("name", "test1"))
	t.Log("test2 : ", cr.Index("name", "test2"))
	t.Log("test3 : ", cr.Index("name", "test3"))
}

func TestIndex(t *testing.T) {
	cr, _ := New(Address{})
	cr.Read("test.csv")

	for i := 1001; i <= 1003; i++ {
		adderss := cr.Index("id", i).(*Address)
		if adderss.Id != i {
			t.Fatal(i, ": ", "adderss.Id != i")
		}
	}
	adderss := cr.Index("id", 1004)
	if adderss != nil {
		t.Fatal(1004, "adderss != nil")
	}

	names := []string{"John Done", "test1", "test2"}

	for _, v := range names {
		adderss := cr.Index("name", v).(*Address)
		if adderss.Name != v {
			t.Fatal(v, " : ", adderss.Name != v)
		}
	}

	adderss = cr.Index("name", "test4")
	if adderss != nil {
		t.Fatal(1004, "adderss != nil")
	}
}
