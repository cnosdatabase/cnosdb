package buildtsi

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/index/tsi1"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const defaultBatchSize = 10000

type Options struct {
	dataDir string
	walDir  string
	//
	Stderr          io.Writer
	Stdout          io.Writer
	Verbose         bool
	Logger          *zap.Logger
	concurrency     int
	databaseFilter  string
	retentionFilter string
	shardFilter     string
	maxLogFileSize  int64
	maxCacheSize    uint64
	batchSize       int
}

func NewOptions() *Options {
	return &Options{
		Stderr:      os.Stderr,
		Stdout:      os.Stdout,
		Logger:      logger.NewLoggerWithWriter(os.Stderr),
		batchSize:   defaultBatchSize,
		concurrency: runtime.GOMAXPROCS(0),
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "buildtsi",
		Short: "Builds TSI (Time Series Index) disk-based shard index files and associated series files.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(opt.dataDir, opt.walDir)
		},
	}
	c.PersistentFlags().StringVar(&opt.dataDir, "datadir", "", "data directory")
	c.PersistentFlags().StringVar(&opt.walDir, "waldir", "", "WAL directory")

	c.PersistentFlags().IntVar(&opt.concurrency, "concurrency", runtime.GOMAXPROCS(0), "Number of workers to dedicate to shard index building. Defaults to GOMAXPROCS")
	c.PersistentFlags().StringVar(&opt.databaseFilter, "database", "", "optional: database name")
	c.PersistentFlags().StringVar(&opt.retentionFilter, "retention", "", "optional: retention policy")
	c.PersistentFlags().StringVar(&opt.shardFilter, "shard", "", "optional: shard id")
	c.PersistentFlags().Int64Var(&opt.maxLogFileSize, "max-log-file-size", tsdb.DefaultMaxIndexLogFileSize, "optional: maximum log file size")
	c.PersistentFlags().Uint64Var(&opt.maxCacheSize, "max-cache-size", tsdb.DefaultCacheMaxMemorySize, "optional: maximum cache size")
	c.PersistentFlags().IntVar(&opt.batchSize, "batch-size", defaultBatchSize, "optional: set the size of the batches we write to the index. Setting this can have adverse affects on performance and heap requirements")
	c.PersistentFlags().BoolVar(&opt.Verbose, "v", false, "verbose")
	c.PersistentFlags().SetOutput(opt.Stdout)
	/*
	 */

	return c
}

func run(dataDir, walDir string) error {
	if isRoot() {
		fmt.Println("You are currently running as root. This will build your")
		fmt.Println("index files with root ownership and will be inaccessible")
		fmt.Println("if you run cnosdb as a non-root user. You should run")
		fmt.Println("buildtsi as the same user you are running cnosdb.")
		fmt.Print("Are you sure you want to continue? (y/N): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("operation aborted")
		}
	}

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		name := fi.Name()
		if !fi.IsDir() {
			continue
		} else if opt.databaseFilter != "" && name != opt.databaseFilter {
			continue
		}

		if err := opt.processDatabase(name, filepath.Join(dataDir, name), filepath.Join(walDir, name)); err != nil {
			return err
		}
	}

	return nil
}

func isRoot() bool {
	user, _ := user.Current()
	return user != nil && user.Username == "root"
}

func (o *Options) processDatabase(dbName, dataDir, walDir string) error {
	o.Logger.Info("Rebuilding database", zap.String("name", dbName))

	sfile := tsdb.NewSeriesFile(filepath.Join(dataDir, tsdb.SeriesFileDirectory))
	sfile.Logger = o.Logger
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		rpName := fi.Name()
		if !fi.IsDir() {
			continue
		} else if rpName == tsdb.SeriesFileDirectory {
			continue
		} else if o.retentionFilter != "" && rpName != o.retentionFilter {
			continue
		}

		if err := o.processRetentionPolicy(sfile, dbName, rpName, filepath.Join(dataDir, rpName), filepath.Join(walDir, rpName)); err != nil {
			return err
		}
	}

	return nil
}

func (o *Options) processRetentionPolicy(sfile *tsdb.SeriesFile, dbName, rpName, dataDir, walDir string) error {
	o.Logger.Info("Rebuilding retention policy", logger.Database(dbName), logger.RetentionPolicy(rpName))

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	type shard struct {
		ID   uint64
		Path string
	}

	var shards []shard

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		} else if o.shardFilter != "" && fi.Name() != o.shardFilter {
			continue
		}

		shardID, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}

		shards = append(shards, shard{shardID, fi.Name()})
	}

	errC := make(chan error, len(shards))
	var maxi uint32 // index of maximum shard being worked on.
	for k := 0; k < o.concurrency; k++ {
		go func() {
			for {
				i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next partition to work on.
				if i >= len(shards) {
					return // No more work.
				}

				id, name := shards[i].ID, shards[i].Path
				log := o.Logger.With(logger.Database(dbName), logger.RetentionPolicy(rpName), logger.Shard(id))
				errC <- IndexShard(sfile, filepath.Join(dataDir, name), filepath.Join(walDir, name), o.maxLogFileSize, o.maxCacheSize, o.batchSize, log, o.Verbose)
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

func IndexShard(sfile *tsdb.SeriesFile, dataDir, walDir string, maxLogFileSize int64, maxCacheSize uint64, batchSize int, log *zap.Logger, verboseLogging bool) error {
	log.Info("Rebuilding shard")

	// Check if shard already has a TSI index.
	indexPath := filepath.Join(dataDir, "index")
	log.Info("Checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		log.Info("tsi1 index already exists, skipping", zap.String("path", indexPath))
		return nil
	}

	log.Info("Opening shard")

	// Remove temporary index files if this is being re-run.
	tmpPath := filepath.Join(dataDir, ".index")
	log.Info("Cleaning up partial index from previous run, if any")
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}

	// Open TSI index in temporary path.
	tsiIndex := tsi1.NewIndex(sfile, "",
		tsi1.WithPath(tmpPath),
		tsi1.WithMaximumLogFileSize(maxLogFileSize),
		tsi1.DisableFsync(),
		// Each new series entry in a log file is ~12 bytes so this should
		// roughly equate to one flush to the file for every batch.
		tsi1.WithLogFileBufferSize(12*batchSize),
	)

	tsiIndex.WithLogger(log)

	log.Info("Opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(); err != nil {
		return err
	}
	defer tsiIndex.Close()

	// Write out tsm1 files.
	// Find shard files.
	tsmPaths, err := collectTSMFiles(dataDir)
	if err != nil {
		return err
	}

	log.Info("Iterating over tsm files")
	for _, path := range tsmPaths {
		log.Info("Processing tsm file", zap.String("path", path))
		if err := IndexTSMFile(tsiIndex, path, batchSize, log, verboseLogging); err != nil {
			return err
		}
	}

	// Write out wal files.
	walPaths, err := collectWALFiles(walDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

	} else {
		log.Info("Building cache from wal files")
		cache := tsm1.NewCache(maxCacheSize)
		loader := tsm1.NewCacheLoader(walPaths)
		loader.WithLogger(log)
		if err := loader.Load(cache); err != nil {
			return err
		}

		log.Info("Iterating over cache")
		keysBatch := make([][]byte, 0, batchSize)
		namesBatch := make([][]byte, 0, batchSize)
		tagsBatch := make([]models.Tags, 0, batchSize)

		for _, key := range cache.Keys() {
			seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
			name, tags := models.ParseKeyBytes(seriesKey)

			if verboseLogging {
				log.Info("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))
			}

			keysBatch = append(keysBatch, seriesKey)
			namesBatch = append(namesBatch, name)
			tagsBatch = append(tagsBatch, tags)

			// Flush batch?
			if len(keysBatch) == batchSize {
				if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
					return fmt.Errorf("problem creating series: (%s)", err)
				}
				keysBatch = keysBatch[:0]
				namesBatch = namesBatch[:0]
				tagsBatch = tagsBatch[:0]
			}
		}

		// Flush any remaining series in the batches
		if len(keysBatch) > 0 {
			if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = nil
			namesBatch = nil
			tagsBatch = nil
		}
	}

	// Attempt to compact the index & wait for all compactions to complete.
	log.Info("compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	log.Info("Closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	log.Info("Moving tsi to permanent location")
	return os.Rename(tmpPath, indexPath)
}

func IndexTSMFile(index *tsi1.Index, path string, batchSize int, log *zap.Logger, verboseLogging bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		log.Warn("Unable to read, skipping", zap.String("path", path), zap.Error(err))
		return nil
	}
	defer r.Close()

	keysBatch := make([][]byte, 0, batchSize)
	namesBatch := make([][]byte, 0, batchSize)
	tagsBatch := make([]models.Tags, batchSize)
	var ti int
	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		var name []byte
		name, tagsBatch[ti] = models.ParseKeyBytesWithTags(seriesKey, tagsBatch[ti])

		if verboseLogging {
			log.Info("Series", zap.String("name", string(name)), zap.String("tags", tagsBatch[ti].String()))
		}

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, name)
		ti++

		// Flush batch?
		if len(keysBatch) == batchSize {
			if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti]); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			ti = 0 // Reset tags.
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti]); err != nil {
			return fmt.Errorf("problem creating series: (%s)", err)
		}
	}
	return nil
}

func collectTSMFiles(path string) ([]string, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != "."+tsm1.TSMFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}

func collectWALFiles(path string) ([]string, error) {
	if path == "" {
		return nil, os.ErrNotExist
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != "."+tsm1.WALFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}
