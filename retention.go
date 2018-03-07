package carbonx

import "github.com/hnakamur/carbonx/carbonzipperpb3"

func sameInfo(info1, info2 carbonzipperpb3.InfoResponse) bool {
	return info1.Name == info2.Name &&
		info1.AggregationMethod == info2.AggregationMethod &&
		info1.MaxRetention == info2.MaxRetention &&
		info1.XFilesFactor == info2.XFilesFactor &&
		sameRetentions(info1.Retentions, info2.Retentions)
}

func sameRetentions(r1, r2 []carbonzipperpb3.Retention) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r1e := range r1 {
		r2e := r2[i]
		if !sameRetention(r1e, r2e) {
			return false
		}
	}
	return true
}

func sameRetention(r1, r2 carbonzipperpb3.Retention) bool {
	return r1.SecondsPerPoint == r2.SecondsPerPoint &&
		r1.NumberOfPoints == r2.NumberOfPoints
}
