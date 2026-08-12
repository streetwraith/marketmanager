package esi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Order is one market order, exactly as ESI returns it. No derived fields.
type Order struct {
	OrderID    int64 `json:"order_id"`
	TypeID     int64 `json:"type_id"`
	LocationID int64 `json:"location_id"`
	SystemID   int64 `json:"system_id"`
	IsBuyOrder bool  `json:"is_buy_order"`
	// Price is kept as the raw JSON text and handed to a numeric(20,2) column
	// unchanged. Decoding to float64 first would risk drift on a value that must
	// stay exact.
	Price        json.Number `json:"price"`
	VolumeRemain int64       `json:"volume_remain"`
	VolumeTotal  int64       `json:"volume_total"`
	MinVolume    int32       `json:"min_volume"`
	Duration     int32       `json:"duration"`
	Range        string      `json:"range"`
	Issued       time.Time   `json:"issued"`
}

// OrderPage is one page of a region's order book, plus the headers that decide
// scheduling and page-set consistency.
type OrderPage struct {
	Orders       []Order
	Pages        int
	Expires      time.Time
	LastModified time.Time
	Status       int
}

// OrdersPage fetches one page of a region's orders.
func (c *Client) OrdersPage(ctx context.Context, regionID int64, page int) (*OrderPage, error) {
	path := fmt.Sprintf("/markets/%d/orders?page=%d", regionID, page)
	var orders []Order
	r, err := c.getJSON(ctx, path, &orders)
	if r == nil {
		return nil, err
	}
	p := &OrderPage{
		Pages:        r.Pages,
		Expires:      r.Expires,
		LastModified: r.LastModified,
		Status:       r.Status,
	}
	if err != nil {
		return p, err
	}
	p.Orders = orders
	return p, nil
}
