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

	blobAnnounceInMeter  = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/announces/in", nil)
	blobAnnounceDOSMeter = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/announces/dos", nil)

	blobRequestOutMeter     = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/request/out", nil)
	blobRequestFailMeter    = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/request/fail", nil)
	blobRequestDoneMeter    = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/request/done", nil)
	blobRequestTimeoutMeter = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/request/timeout", nil)

	blobReplyInMeter     = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/replies/in", nil)
	blobReplyRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/blobFetcher/blob/replies/reject", nil)

	blobFetcherWaitingPeers   = metrics.NewRegisteredGauge("eth/fetcher/blobFetcher/blob/waiting/peers", nil)  // availability waiting
	blobFetcherWaitingHashes  = metrics.NewRegisteredGauge("eth/fetcher/blobFetcher/blob/waiting/hashes", nil) // availability waiting
	blobFetcherQueueingPeers  = metrics.NewRegisteredGauge("eth/fetcher/blobFetcher/blob/queueing/peers", nil) // queued + fetching txs
	blobFetcherQueueingHashes = metrics.NewRegisteredGauge("eth/fetcher/blobFetcher/blob/queueing/hashes", nil)
	blobFetcherFetchingPeers  = metrics.NewRegisteredGauge("eth/fetcher/blobFetcher/blob/fetching/peers", nil) // fetching txs
	blobFetcherFetchingHashes = metrics.NewRegisteredGauge("eth/fetcher/blobFetcher/blob/fetching/hashes", nil)
)
