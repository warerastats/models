package trackers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MarketOffer struct {
	ID          bson.ObjectID    `bson:"_id"`
	UserID      bson.ObjectID    `bson:"userId"`
	CountryID   *bson.ObjectID   `bson:"countryId,omitempty"`
	MuID        *bson.ObjectID   `bson:"muId,omitempty"`
	ItemCode    string           `bson:"itemCode"`
	Side        enums.MarketSide `bson:"side"`
	Quantity    int              `bson:"quantity"`
	Fulfilled   int              `bson:"fulfilled"`
	Cancelled   bool             `bson:"cancelled"`
	Price       float64          `bson:"price"`
	Since       time.Time        `bson:"since"`
	LastUpdated time.Time        `bson:"lastUpdated"`
}

type MarketOfferStore struct {
	coll *mongo.Collection
}

func NewMarketOfferStore(ctx context.Context, db *mongo.Database) *MarketOfferStore {
	store := &MarketOfferStore{
		coll: db.Collection("market_offers"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MarketOfferStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on market_offers.userId", "error", err)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "itemCode", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on market_offers.itemCode", "error", err)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
			{Key: "side", Value: 1},
			{Key: "userId", Value: 1},
			{Key: "since", Value: 1},
		},
	})
	if err != nil {
		slog.Error("Failed creating compound index on market_offers (itemCode, side, userId, since)", "error", err)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
			{Key: "side", Value: 1},
			{Key: "cancelled", Value: 1},
		},
	})
	if err != nil {
		slog.Error("Failed creating compound index on market_offers (itemCode, side, cancelled)", "error", err)
	}
}

// objectIDOrNull marshals to BSON null when p is nil; the caller's pipeline
// uses $ifNull to preserve any existing field value in that case.
func objectIDOrNull(p *bson.ObjectID) any {
	if p == nil {
		return nil
	}
	return *p
}

// UpsertFromAPI applies an API top-orders sighting to the offer. It is
// keyed on _id (the canonical server-assigned offer ID). On insert the doc
// is created with quantity = apiRemaining, fulfilled = 0, cancelled = false.
// On update, quantity is raised monotonically to max(quantity, fulfilled+apiRemaining)
// and fulfilled is raised to max(fulfilled, quantity-apiRemaining) so we
// catch trades that landed between sweeps. countryId/muId are filled in only
// when provided; an existing value is never cleared.
//
// Returns wasInsert = true when the doc did not previously exist, so callers
// can run ReconcileSynthetic to absorb a trade-derived placeholder.
func (s *MarketOfferStore) UpsertFromAPI(
	ctx context.Context,
	id bson.ObjectID,
	userID bson.ObjectID,
	countryID *bson.ObjectID,
	muID *bson.ObjectID,
	itemCode string,
	side enums.MarketSide,
	apiRemaining int,
	price float64,
	since time.Time,
) (bool, error) {
	now := time.Now().UTC()

	pipeline := bson.A{
		bson.M{"$set": bson.M{
			"userId":      userID,
			"itemCode":    itemCode,
			"side":        side,
			"price":       price,
			"since":       since,
			"lastUpdated": now,
			"countryId":   bson.M{"$ifNull": bson.A{objectIDOrNull(countryID), "$countryId"}},
			"muId":        bson.M{"$ifNull": bson.A{objectIDOrNull(muID), "$muId"}},
			"fulfilled":   bson.M{"$ifNull": bson.A{"$fulfilled", 0}},
			"cancelled":   bson.M{"$ifNull": bson.A{"$cancelled", false}},
			"quantity": bson.M{"$max": bson.A{
				bson.M{"$ifNull": bson.A{"$quantity", apiRemaining}},
				bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$fulfilled", 0}}, apiRemaining}},
			}},
		}},
		bson.M{"$set": bson.M{
			"fulfilled": bson.M{"$max": bson.A{
				"$fulfilled",
				bson.M{"$subtract": bson.A{"$quantity", apiRemaining}},
			}},
		}},
	}

	res, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: id}},
		pipeline,
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return false, err
	}
	return res.UpsertedCount > 0, nil
}

// FindByMatch returns the offer (if any) for a given (userID, itemCode, side, since).
// Used by the trading ingester to locate the maker offer.
// Returns mongo.ErrNoDocuments cleanly when nothing matches.
func (s *MarketOfferStore) FindByMatch(
	ctx context.Context,
	itemCode string,
	side enums.MarketSide,
	since time.Time,
	userID bson.ObjectID,
) (*MarketOffer, error) {
	var offer MarketOffer
	err := s.coll.FindOne(ctx, bson.D{
		{Key: "itemCode", Value: itemCode},
		{Key: "side", Value: side},
		{Key: "since", Value: since},
		{Key: "userId", Value: userID},
	}).Decode(&offer)
	if err != nil {
		return nil, err
	}
	return &offer, nil
}

// RecordFill increments fulfilled by qty and raises quantity to at least the
// new fulfilled value. Used when a TradeTransaction matches an existing offer.
func (s *MarketOfferStore) RecordFill(ctx context.Context, id bson.ObjectID, qty int) error {
	now := time.Now().UTC()
	pipeline := bson.A{
		bson.M{"$set": bson.M{
			"fulfilled":   bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$fulfilled", 0}}, qty}},
			"lastUpdated": now,
		}},
		bson.M{"$set": bson.M{
			"quantity": bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$quantity", 0}}, "$fulfilled"}},
		}},
	}
	_, err := s.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, pipeline)
	return err
}

// CreateSynthetic idempotently inserts a placeholder offer for a trade we
// observed before the API surfaced its real _id. The caller derives a
// deterministic syntheticID from the trade ID so re-ingestion is a no-op.
func (s *MarketOfferStore) CreateSynthetic(
	ctx context.Context,
	syntheticID bson.ObjectID,
	userID bson.ObjectID,
	countryID *bson.ObjectID,
	muID *bson.ObjectID,
	itemCode string,
	side enums.MarketSide,
	price float64,
	since time.Time,
	tradedQty int,
) error {
	now := time.Now().UTC()
	doc := MarketOffer{
		ID:          syntheticID,
		UserID:      userID,
		CountryID:   countryID,
		MuID:        muID,
		ItemCode:    itemCode,
		Side:        side,
		Price:       price,
		Since:       since,
		Quantity:    tradedQty,
		Fulfilled:   tradedQty,
		Cancelled:   false,
		LastUpdated: now,
	}
	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: syntheticID}},
		bson.D{{Key: "$setOnInsert", Value: doc}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// ReconcileSynthetic looks for any other offer with the same
// (userID, itemCode, side, since) as realID and, if found, ports its
// fulfilled/quantity counters onto realID via $max and deletes the
// synthetic. No-op when no synthetic exists. Sequential ops, no
// transaction; the worst-case race is double-counting at most one fill,
// which the next API sweep will self-heal.
func (s *MarketOfferStore) ReconcileSynthetic(
	ctx context.Context,
	realID bson.ObjectID,
	userID bson.ObjectID,
	itemCode string,
	side enums.MarketSide,
	since time.Time,
) error {
	var synth MarketOffer
	err := s.coll.FindOne(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$ne", Value: realID}}},
		{Key: "itemCode", Value: itemCode},
		{Key: "side", Value: side},
		{Key: "since", Value: since},
		{Key: "userId", Value: userID},
	}).Decode(&synth)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil
		}
		return err
	}

	_, err = s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: realID}},
		bson.D{{Key: "$max", Value: bson.D{
			{Key: "fulfilled", Value: synth.Fulfilled},
			{Key: "quantity", Value: synth.Quantity},
		}}},
	)
	if err != nil {
		return err
	}

	_, err = s.coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: synth.ID}})
	return err
}

// MarkCancelledOutsideBand flips cancelled=true for offers in (itemCode, side)
// that are not in seenIDs, are not yet fully filled, and lie outside the
// confidence band defined by worstReturnedPrice when exhaustive is false.
//
// When exhaustive is true (API returned fewer than the requested limit), every
// non-cancelled, non-filled offer for the (itemCode, side) that is absent from
// seenIDs is cancelled. When exhaustive is false, the price filter applies:
// SELL → price < worstReturnedPrice (strictly cheaper than the worst returned),
// BUY  → price > worstReturnedPrice (strictly more expensive than the worst).
// Equal-priced offers are left untouched (strict band per design).
func (s *MarketOfferStore) MarkCancelledOutsideBand(
	ctx context.Context,
	itemCode string,
	side enums.MarketSide,
	seenIDs []bson.ObjectID,
	worstReturnedPrice *float64,
	exhaustive bool,
) (int64, error) {
	filter := bson.D{
		{Key: "itemCode", Value: itemCode},
		{Key: "side", Value: side},
		{Key: "cancelled", Value: false},
		{Key: "$expr", Value: bson.D{{Key: "$lt", Value: bson.A{"$fulfilled", "$quantity"}}}},
	}
	if len(seenIDs) > 0 {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$nin", Value: seenIDs}}})
	}
	if !exhaustive {
		if worstReturnedPrice == nil {
			// We have a 100-result page but no worst price (shouldn't happen);
			// be safe and skip — we can't bound which docs were certainly cancelled.
			return 0, nil
		}
		switch side {
		case enums.SELL:
			filter = append(filter, bson.E{Key: "price", Value: bson.D{{Key: "$lt", Value: *worstReturnedPrice}}})
		case enums.BUY:
			filter = append(filter, bson.E{Key: "price", Value: bson.D{{Key: "$gt", Value: *worstReturnedPrice}}})
		}
	}

	now := time.Now().UTC()
	res, err := s.coll.UpdateMany(
		ctx,
		filter,
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "cancelled", Value: true},
			{Key: "lastUpdated", Value: now},
		}}},
	)
	if err != nil {
		return 0, err
	}
	return res.ModifiedCount, nil
}
