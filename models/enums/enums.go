package enums

type ItemStatus string

const (
	PERFECT    ItemStatus = "PERFECT"
	ONSALE     ItemStatus = "ONSALE"
	USED       ItemStatus = "USED"
	BROKEN     ItemStatus = "BROKEN"
	DISMANTLED ItemStatus = "DISMANTLED"
)

type Side string

const (
	ATTACKER Side = "ATTACKER"
	DEFENDER Side = "DEFENDER"
)

type MarketSide string

const (
	BUY  MarketSide = "BUY"
	SELL MarketSide = "SELL"
)
