package scrape

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/session"
)

func (ts *targetScraper) getSessionInfo(ctx context.Context) (*session.SessionInfo, error) {
	extraParams := url.Values{}
	extraParams.Add("session", "true")

	req, err := http.NewRequest("GET", ts.newURL(extraParams).String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", ts.acceptHeader)
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(ts.timeout.Seconds(), 'f', -1, 64))

	resp, err := ts.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	ss := &session.SessionInfo{}
	err = json.Unmarshal(body, ss)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

// URL returns a copy of the target's URL.
func (t *Target) newURL(extraParams url.Values) *url.URL {
	params := url.Values{}

	for k, v := range t.params {
		params[k] = make([]string, len(v))
		copy(params[k], v)
	}
	for k, v := range extraParams {
		params[k] = make([]string, len(v))
		copy(params[k], v)
	}
	t.labels.Range(func(l labels.Label) {
		if !strings.HasPrefix(l.Name, model.ParamLabelPrefix) {
			return
		}
		ks := l.Name[len(model.ParamLabelPrefix):]

		if len(params[ks]) > 0 {
			params[ks][0] = l.Value
		} else {
			params[ks] = []string{l.Value}
		}
	})

	return &url.URL{
		Scheme:   t.labels.Get(model.SchemeLabel),
		Host:     t.labels.Get(model.AddressLabel),
		Path:     t.labels.Get(model.MetricsPathLabel),
		RawQuery: params.Encode(),
	}
}

func (s *targetScraper) multiScrape(_ context.Context, extraParams url.Values, w io.Writer) (string, error) {
	req, err := http.NewRequest("GET", s.newURL(extraParams).String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Accept", s.acceptHeader)
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(s.timeout.Seconds(), 'f', -1, 64))

	//req = req.WithContext(ctx)
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	if s.bodySizeLimit <= 0 {
		s.bodySizeLimit = math.MaxInt64
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		n, err := io.Copy(w, io.LimitReader(resp.Body, s.bodySizeLimit))
		if err != nil {
			return "", err
		}
		if n >= s.bodySizeLimit {
			targetScrapeExceededBodySizeLimit.Inc()
			return "", errBodySizeLimit
		}
		return resp.Header.Get("Content-Type"), nil
	}

	gzipr, err := GetGzipReader(resp.Body)
	if err != nil {
		return "", err
	}

	n, err := io.Copy(w, io.LimitReader(gzipr.gzipr, s.bodySizeLimit))
	PutGzipReader(gzipr)

	if err != nil {
		return "", err
	}
	if n >= s.bodySizeLimit {
		targetScrapeExceededBodySizeLimit.Inc()
		return "", errBodySizeLimit
	}
	return resp.Header.Get("Content-Type"), nil
}

func (sl *scrapeLoop) multiScrapeAndReport(last, appendTime time.Time, errc chan<- error) time.Time {
	start := time.Now()

	fmt.Printf("\n\nrun4.1")

	// Only record after the first scrape.
	if !last.IsZero() {
		targetIntervalLength.WithLabelValues(sl.interval.String()).Observe(
			time.Since(last).Seconds(),
		)
	}

	_starttime := time.Now()
	ss, err := sl.scraper.getSessionInfo(sl.parentCtx)
	if err != nil {
		errc <- err
		fmt.Printf("\n[hackathon] unable to get session info %v", err.Error())
		return start
	}

	fmt.Printf("\n\nrun4.2 %v => %v", ss.SessionID, ss.Size)

	if ss.Size > 0 {
		scrapeCtx, cancel := context.WithTimeout(sl.parentCtx, sl.timeout)

		fn1 := func(sr *scrapeResponse) {
			sl.onlyAppend(sr, appendTime, errc)
		}
		queue := newQueue(context.Background(), fn1, 2000)

		fn := func(start, end int) error {
			//scrape with parameters session=true, sessionid=, start, end
			sr := &scrapeResponse{
				buf:       &bytes.Buffer{},
				startTime: time.Now(),
			}

			extraParams := url.Values{}
			extraParams.Set("session", "true")
			extraParams.Set("sessionid", ss.SessionID)
			extraParams.Set("start", strconv.Itoa(start))
			extraParams.Set("end", strconv.Itoa(end))

			fmt.Printf("\n[hack_start] sessionid:[%v] start:[%v] end:[%v]", ss.SessionID, start, end)
			sr.contentType, sr.err = sl.scraper.multiScrape(sl.parentCtx, extraParams, sr.buf)
			fmt.Printf("\n[hack_end] sessionid:[%v] start:[%v] end:[%v] ts:[%v] buf:[%v] err:[%v]", ss.SessionID, start, end, time.Since(sr.startTime), sr.buf.Len(), err)

			level.Info(sl.l).Log("[hackathon]", "",
				"sessionid", ss.SessionID,
				"start", start, "end", end,
				"ts", time.Since(sr.startTime),
				"buf", sr.buf.Len(), "err", err)

			queue.push(sr)

			return sr.err
		}

		threads := sl.session.Threads
		if threads <= 0 {
			threads = 10
		}
		sc := session.NewSessionClient(scrapeCtx, int64(ss.Size), sl.session.BatchSize, sl.session.Threads, fn)
		go queue.process()

		//cleanup
		sc.Wait()
		queue.close()
		cancel()
	}

	level.Info(sl.l).Log("[hackathon]", "", "sessionid", ss.SessionID, "size", ss.Size, "ts", time.Since(_starttime))
	//fmt.Printf("\n[hackathon] sessionid:[%v] size:[%v] ts:[%v]", ss.SessionID, ss.Size, time.Since(_starttime))

	return start
}

func (sl *scrapeLoop) onlyAppend(sr *scrapeResponse, appendTime time.Time, errc chan<- error) {
	var total, added, seriesAdded, bytes int
	var err, appErr, scrapeErr error

	app := sl.appender(sl.appenderCtx)
	defer func() {
		if err != nil {
			app.Rollback()
			return
		}
		err = app.Commit()
		if err != nil {
			level.Error(sl.l).Log("msg", "Scrape commit failed", "err", err)
		}
	}()

	defer func() {
		if err = sl.report(app, appendTime, time.Since(sr.startTime), total, added, seriesAdded, bytes, scrapeErr); err != nil {
			level.Warn(sl.l).Log("msg", "Appending scrape report failed", "err", err)
		}
	}()

	if forcedErr := sl.getForcedError(); forcedErr != nil {
		scrapeErr = forcedErr
		// Add stale markers.
		if _, _, _, err := sl.append(app, []byte{}, "", appendTime); err != nil {
			app.Rollback()
			app = sl.appender(sl.appenderCtx)
			level.Warn(sl.l).Log("msg", "Append failed", "err", err)
		}
		if errc != nil {
			errc <- forcedErr
		}
	}

	if scrapeErr == nil {
		bytes = sr.buf.Len()
		// // NOTE: There were issues with misbehaving clients in the past
		// // that occasionally returned empty results. We don't want those
		// // to falsely reset our buffer size.
		// if len(b) > 0 {
		// 	sl.lastScrapeSize = len(b)
		// }
		// bytes = len(b)
	} else {
		level.Debug(sl.l).Log("msg", "Scrape failed", "err", scrapeErr)
		if errc != nil {
			errc <- scrapeErr
		}
		if errors.Is(scrapeErr, errBodySizeLimit) {
			bytes = -1
		}
	}

	total, added, seriesAdded, appErr = sl.append(app, sr.buf.Bytes(), sr.contentType, appendTime)
	if appErr != nil {
		app.Rollback()
		app = sl.appender(sl.appenderCtx)
		level.Debug(sl.l).Log("msg", "Append failed", "err", appErr)
		// The append failed, probably due to a parse error or sample limit.
		// Call sl.append again with an empty scrape to trigger stale markers.
		if _, _, _, err := sl.append(app, []byte{}, "", appendTime); err != nil {
			app.Rollback()
			app = sl.appender(sl.appenderCtx)
			level.Warn(sl.l).Log("msg", "Append failed", "err", err)
		}
	}

	if scrapeErr == nil {
		scrapeErr = appErr
	}
}

type scrapeResponse struct {
	buf         *bytes.Buffer
	contentType string
	err         error
	startTime   time.Time
}

type queue struct {
	ctx context.Context
	ch  chan *scrapeResponse
	fn  func(*scrapeResponse)
}

func newQueue(ctx context.Context, fn func(*scrapeResponse), size int) *queue {
	return &queue{
		ctx: ctx,
		ch:  make(chan *scrapeResponse, size),
		fn:  fn,
	}
}

func (q *queue) push(sr *scrapeResponse) {
	q.ch <- sr
}

func (q *queue) process() {
	for response := range q.ch {
		if q.fn != nil {
			q.fn(response)
		}
	}
}

func (q *queue) close() {
	close(q.ch)
	fmt.Printf("\n[hackathon] queue closed")
}

type gzipreader struct {
	buf   *bufio.Reader
	gzipr *gzip.Reader
}

func (gz *gzipreader) Reset(reader io.Reader) (err error) {
	//reader
	if gz.buf == nil {
		gz.buf = bufio.NewReader(reader)
	} else {
		gz.buf.Reset(reader)
	}

	//gzip
	if gz.gzipr == nil {
		gz.gzipr, err = gzip.NewReader(gz.buf)
	} else {
		err = gz.gzipr.Reset(gz.buf)
	}

	if err != nil {
		gz.gzipr = nil
	}
	return
}

func (gz *gzipreader) Close() {
	if gz.gzipr != nil {
		gz.gzipr.Close()
	}
}

// Define a sync.Pool for gzip readers
var gzipReaderPool = sync.Pool{
	New: func() interface{} {
		return &gzipreader{}
	},
}

// GetGzipReader gets a gzip reader from the pool
func GetGzipReader(r io.Reader) (*gzipreader, error) {
	gzipr := gzipReaderPool.Get().(*gzipreader)
	err := gzipr.Reset(r)
	if err != nil {
		return nil, err
	}
	return gzipr, nil
}

// PutGzipReader returns the gzip reader to the pool
func PutGzipReader(gzipr *gzipreader) {
	gzipr.Close()
	gzipReaderPool.Put(gzipr)
}
