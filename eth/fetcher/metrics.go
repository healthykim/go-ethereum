package fetcher

import "github.com/ethereum/go-ethereum/metrics"

var (
	txAnnounceInMeter          = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/in", nil)
	txAnnounceKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/known", nil)
	txAnnounceUnderpricedMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/underpriced", nil)
	txAnnounceDOSMeter         = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/dos", nil)

	txBroadcastInMeter          = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/in", nil)
	txBroadcastKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/known", nil)
	txBroadcastUnderpricedMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/underpriced", nil)
	txBroadcastOtherRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/otherreject", nil)

	txRequestOutMeter     = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/out", nil)
	txRequestFailMeter    = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/fail", nil)
	txRequestDoneMeter    = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/done", nil)
	txRequestTimeoutMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/timeout", nil)

	txReplyInMeter          = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/in", nil)
	txReplyKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/known", nil)
	txReplyUnderpricedMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/underpriced", nil)
	txReplyOtherRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/otherreject", nil)

	txFetcherWaitingPeers   = metrics.NewRegisteredGauge("eth/fetcher/transaction/waiting/peers", nil)
	txFetcherWaitingHashes  = metrics.NewRegisteredGauge("eth/fetcher/transaction/waiting/hashes", nil)
	txFetcherQueueingPeers  = metrics.NewRegisteredGauge("eth/fetcher/transaction/queueing/peers", nil)
	txFetcherQueueingHashes = metrics.NewRegisteredGauge("eth/fetcher/transaction/queueing/hashes", nil)
	txFetcherFetchingPeers  = metrics.NewRegisteredGauge("eth/fetcher/transaction/fetching/peers", nil)
	txFetcherFetchingHashes = metrics.NewRegisteredGauge("eth/fetcher/transaction/fetching/hashes", nil)

	blobAnnounceInMeter  = metrics.NewRegisteredMeter("eth/fetcher/blob/announces/in", nil)
	blobAnnounceDOSMeter = metrics.NewRegisteredMeter("eth/fetcher/blob/announces/dos", nil)

	blobRequestOutMeter     = metrics.NewRegisteredMeter("eth/fetcher/blob/request/out", nil)
	blobRequestFailMeter    = metrics.NewRegisteredMeter("eth/fetcher/blob/request/fail", nil)
	blobRequestDoneMeter    = metrics.NewRegisteredMeter("eth/fetcher/blob/request/done", nil)
	blobRequestTimeoutMeter = metrics.NewRegisteredMeter("eth/fetcher/blob/request/timeout", nil)

	blobReplyInMeter     = metrics.NewRegisteredMeter("eth/fetcher/blob/replies/in", nil)
	blobReplyRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/blob/replies/reject", nil)

	blobFetcherWaitingPeers   = metrics.NewRegisteredGauge("eth/fetcher/blob/waiting/peers", nil)  // availability waiting
	blobFetcherWaitingHashes  = metrics.NewRegisteredGauge("eth/fetcher/blob/waiting/hashes", nil) // availability waiting
	blobFetcherQueueingPeers  = metrics.NewRegisteredGauge("eth/fetcher/blob/queueing/peers", nil) // queued + fetching txs
	blobFetcherQueueingHashes = metrics.NewRegisteredGauge("eth/fetcher/blob/queueing/hashes", nil)
	blobFetcherFetchingPeers  = metrics.NewRegisteredGauge("eth/fetcher/blob/fetching/peers", nil) // fetching txs
	blobFetcherFetchingHashes = metrics.NewRegisteredGauge("eth/fetcher/blob/fetching/hashes", nil)

	blobFetcherWaitTime  = metrics.NewRegisteredHistogram("eth/fetcher/blob/wait/time", nil, metrics.NewExpDecaySample(1028, 0.015))  // total availability wait time
	blobFetcherFetchTime = metrics.NewRegisteredHistogram("eth/fetcher/blob/fetch/time", nil, metrics.NewExpDecaySample(1028, 0.015)) // total fetch time (time taken to collect all cells needed)
)
