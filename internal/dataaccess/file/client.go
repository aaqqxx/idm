package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/maxuanquang/idm/internal/configs"
	"github.com/maxuanquang/idm/internal/download"
	"github.com/maxuanquang/idm/internal/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type Client interface {
	Write(ctx context.Context, fileName string) (io.WriteCloser, error)
	Read(ctx context.Context, fileName string) (io.ReadCloser, error)
	// Sink returns a random-access destination for the segmented engine. The
	// caller owns the sink and must Close it after Commit/Abort.
	Sink(ctx context.Context, fileName string) (download.Sink, error)
}

func NewClient(downloadConfig configs.Download, logger *zap.Logger) (Client, error) {
	switch downloadConfig.Mode {
	case configs.DownloadModeLocal:
		return NewLocalClient(downloadConfig, logger)
	case configs.DownloadModeS3:
		return NewS3Client(downloadConfig, logger)
	default:
		return nil, fmt.Errorf("unsupported download mode: %s", downloadConfig.Mode)
	}
}

func NewLocalClient(downloadConfig configs.Download, logger *zap.Logger) (Client, error) {
	if err := os.MkdirAll(downloadConfig.DownloadDirectory, os.ModeDir); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}

	return &localClient{
		downloadDirectory: downloadConfig.DownloadDirectory,
		logger:            logger,
	}, nil
}

type localClient struct {
	downloadDirectory string
	logger            *zap.Logger
}

// Read implements Client.
func (l *localClient) Read(ctx context.Context, fileName string) (io.ReadCloser, error) {
	logger := utils.LoggerWithContext(ctx, l.logger).With(zap.String("file_name", fileName))

	filePath := path.Join(l.downloadDirectory, fileName)
	file, err := os.Open(filePath)
	if err != nil {
		logger.With(zap.Error(err)).Error("can not open file")
		return nil, err
	}

	return file, nil
}

// Write implements Client.
func (l *localClient) Write(ctx context.Context, fileName string) (io.WriteCloser, error) {
	logger := utils.LoggerWithContext(ctx, l.logger).With(zap.String("file_name", fileName))

	filePath := path.Join(l.downloadDirectory, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		logger.With(zap.Error(err)).Error("can not open file")
		return nil, err
	}

	return file, nil
}

// Sink implements Client. The engine writes straight into the final file, so
// no join step is needed afterwards.
func (l *localClient) Sink(ctx context.Context, fileName string) (download.Sink, error) {
	return download.NewFileSink(path.Join(l.downloadDirectory, fileName))
}

func NewS3Client(downloadConfig configs.Download, logger *zap.Logger) (Client, error) {

	minioClient, err := minio.New(
		downloadConfig.Address,
		&minio.Options{
			Creds: credentials.NewStaticV4(
				downloadConfig.Username,
				downloadConfig.Password,
				"",
			),
		},
	)
	if err != nil {
		logger.With(zap.Error(err)).Error("failed to create minio client")
		return nil, err
	}

	// Make a new bucket
	bucketName := downloadConfig.Bucket
	err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(context.Background(), bucketName)
		if errBucketExists == nil && exists {
			logger.Info("bucket existed")
		} else {
			logger.With(zap.Error(err)).Error("failed to create bucket")
			return nil, err
		}
	}

	return &s3Client{
		minioClient: minioClient,
		logger:      logger,
		bucketName:  bucketName,
	}, nil
}

type s3Client struct {
	minioClient *minio.Client
	logger      *zap.Logger
	bucketName  string
}

// Read implements Client.
func (s *s3Client) Read(ctx context.Context, fileName string) (io.ReadCloser, error) {
	logger := utils.LoggerWithContext(ctx, s.logger).With(zap.String("file_name", fileName))

	object, err := s.minioClient.GetObject(ctx, s.bucketName, fileName, minio.GetObjectOptions{})
	if err != nil {
		logger.With(zap.Error(err)).Error("failed to get object")
		return nil, err
	}

	return object, nil
}

// Write implements Client.
func (s *s3Client) Write(ctx context.Context, fileName string) (io.WriteCloser, error) {
	return newS3ReadWriteCloser(
		ctx,
		s.minioClient,
		s.logger,
		s.bucketName,
		fileName,
	)
}

// Sink implements Client. Object stores have no random writes, so the engine
// downloads into a local spool file and the payload is uploaded once on
// Commit. The spool path does not clash between concurrent tasks.
func (s *s3Client) Sink(ctx context.Context, fileName string) (download.Sink, error) {
	tmp, err := os.CreateTemp("", "idm-spool-*")
	if err != nil {
		return nil, fmt.Errorf("s3 sink: create spool file: %w", err)
	}
	spoolPath := tmp.Name()
	_ = tmp.Close()

	fs, err := download.NewFileSink(spoolPath)
	if err != nil {
		_ = os.Remove(spoolPath)
		return nil, err
	}
	return &s3Sink{
		minioClient: s.minioClient,
		logger:      s.logger,
		bucketName:  s.bucketName,
		objectName:  fileName,
		spoolPath:   spoolPath,
		file:        fs,
		createdAt:   time.Now(),
	}, nil
}

// s3Sink spools downloaded bytes locally and publishes the object on Commit.
type s3Sink struct {
	minioClient *minio.Client
	logger      *zap.Logger
	bucketName  string
	objectName  string
	spoolPath   string
	file        *download.FileSink
	createdAt   time.Time
}

func (s *s3Sink) WriterAt() io.WriterAt { return s.file.WriterAt() }

func (s *s3Sink) Truncate(size int64) error { return s.file.Truncate(size) }

func (s *s3Sink) Commit(ctx context.Context) error {
	logger := s.logger.With(zap.String("object", s.objectName))
	if err := s.file.Commit(ctx); err != nil {
		return err
	}
	f, err := os.Open(s.spoolPath)
	if err != nil {
		return fmt.Errorf("s3 sink: reopen spool: %w", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("s3 sink: stat spool: %w", err)
	}
	if _, err := s.minioClient.PutObject(ctx, s.bucketName, s.objectName, f, st.Size(), minio.PutObjectOptions{}); err != nil {
		logger.With(zap.Error(err)).Error("failed to publish object")
		return fmt.Errorf("s3 sink: publish: %w", err)
	}
	logger.With(zap.Int64("size", st.Size())).Info("published object")
	_ = os.Remove(s.spoolPath)
	return nil
}

func (s *s3Sink) Abort(ctx context.Context) error {
	_ = s.file.Abort(ctx)
	_ = os.Remove(s.spoolPath)
	return nil
}

func (s *s3Sink) Close() error {
	return nil
}

// SpoolDirHint is referenced by callers that need a scratch directory next to
// a sink. Kept on the client so knowledge of local vs remote stays here.
func SpoolDirHint() string { return filepath.Join(os.TempDir(), "idm-spool") }

func newS3ReadWriteCloser(
	ctx context.Context,
	minioClient *minio.Client,
	logger *zap.Logger,
	bucketName string,
	fileName string,
) (*s3ReadWriteCloser, error) {
	logger = utils.LoggerWithContext(ctx, logger)
	readWriteCloser := &s3ReadWriteCloser{
		buffer:   make([]byte, 0),
		isClosed: false,
	}

	go func() {
		if _, err := minioClient.PutObject(
			ctx,
			bucketName,
			fileName,
			readWriteCloser,
			-1,
			minio.PutObjectOptions{},
		); err != nil {
			logger.With(zap.Error(err)).Error("failed to put object")
		}
	}()

	return readWriteCloser, nil
}

type s3ReadWriteCloser struct {
	buffer   []byte
	isClosed bool
}

func (s *s3ReadWriteCloser) Read(p []byte) (int, error) {
	if len(s.buffer) > 0 {
		readLength := copy(p, s.buffer)
		s.buffer = s.buffer[readLength:]
		return readLength, nil
	}

	if s.isClosed {
		return 0, io.EOF
	}

	return 0, nil
}

func (s *s3ReadWriteCloser) Write(p []byte) (int, error) {
	s.buffer = append(s.buffer, p...)
	return len(p), nil
}

func (s *s3ReadWriteCloser) Close() error {
	s.isClosed = true
	return nil
}
