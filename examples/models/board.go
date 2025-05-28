package models

import (
	"fmt"
	"strings"
)

const (
	AmazonBoard BoardType = iota + 1
	EbayBoard
	WalmartBoard
	ShopifyBoard
	AlibabaBoard
	WishBoard
	// add new board to end
)

var (
	supportBoards = map[BoardType]string{
		AmazonBoard:  "amazon",
		EbayBoard:    "ebay",
		WalmartBoard: "walmart",
		ShopifyBoard: "shopify",
		AlibabaBoard: "alibaba",
		WishBoard:    "wish",
	}
	supportBoardsByName = boardTypeByBoard()
)

func boardTypeByBoard() map[string]BoardType {
	o := make(map[string]BoardType)
	for key, name := range supportBoards {
		o[name] = key
	}
	return o
}

type BoardType uint64

func (b *BoardType) String() string {
	return supportBoards[*b]
}

func GetBoardType(board string) (uint64, error) {
	boardType, ok := supportBoardsByName[strings.ToLower(board)]
	if !ok {
		return 0, fmt.Errorf("unknown board type: %s", board)
	}
	return uint64(boardType), nil
}
