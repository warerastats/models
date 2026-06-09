package candles

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// GetRange returns an item's OHLC candles whose bucketStart falls in [from, to],
// oldest first.
func (s *ItemCandleStore) GetRange(ctx context.Context, itemCode string, from, to time.Time) ([]ItemCandle, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{
			{Key: "itemCode", Value: itemCode},
			{Key: "bucketStart", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lte", Value: to}}},
		},
		options.Find().SetSort(bson.D{{Key: "bucketStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []ItemCandle
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetLatest returns the most recent candle for an item code, or false when none.
func (s *ItemCandleStore) GetLatest(ctx context.Context, itemCode string) (*ItemCandle, bool, error) {
	var out ItemCandle
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "itemCode", Value: itemCode}},
		options.FindOne().SetSort(bson.D{{Key: "bucketStart", Value: -1}}),
	).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &out, true, nil
}

// GetRange returns wage OHLC candles whose bucketStart falls in [from, to],
// oldest first.
func (s *WageCandleStore) GetRange(ctx context.Context, from, to time.Time) ([]WageCandle, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "bucketStart", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lte", Value: to}}}},
		options.Find().SetSort(bson.D{{Key: "bucketStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []WageCandle
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetLatest returns the most recent wage candle, or false when none.
func (s *WageCandleStore) GetLatest(ctx context.Context) (*WageCandle, bool, error) {
	var out WageCandle
	err := s.coll.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "bucketStart", Value: -1}}),
	).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &out, true, nil
}
