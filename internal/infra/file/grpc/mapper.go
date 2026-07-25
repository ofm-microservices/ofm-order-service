package grpc

import (
	"errors"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	filev1 "github.com/ofm-microservices/ofm-common/proto/file/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fileMapper struct {
	log logging.Logger
}

func newFileMapper(log logging.Logger) *fileMapper {
	return &fileMapper{log: log}
}

func (m *fileMapper) ToGetFileURLRequest(fileID string) *filev1.GetFileURLRequest {
	return &filev1.GetFileURLRequest{FileId: fileID}
}

func (m *fileMapper) ToGetFileURLResponse(res *filev1.GetFileURLResponse) string {
	if res == nil {
		return ""
	}
	return res.GetUrl()
}

func (m *fileMapper) ToError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound:
		return err
	default:
		if errors.Is(err, ErrEmptyFileServiceAddress) {
			return err
		}
		m.log.Error("file-service request failed", logging.Operation("grpc.file.map_error"), logging.Err(err))
		return err
	}
}
