package jsonproc

import (
	"fmt"
	"gopkg.in/olivere/elastic.v5"
)

type EsInput struct {
	client    *elastic.Client
	processor *elastic.BulkProcessor
}

func NewEsInput(url string) *EsInput {
	esInput := new(EsInput)
	var err error
	esInput.client, err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(url))
	if err != nil {
		fmt.Println(err)
		return nil
	}

	esInput.processor, err = esInput.client.BulkProcessor().BulkActions(5000).Do()
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return esInput
}

func (this *EsInput) Quit() error {
	if nil != this.processor {
		err := this.processor.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *EsInput) AddData(_index, _type, value string) error {
	if nil == this.processor {
		return fmt.Errorf("es处理已经退出，请重新初始化")
	}

	r := elastic.NewBulkIndexRequest().Index(_index).Type(_type).Doc(value)
	this.processor.Add(r)

	return nil
}
