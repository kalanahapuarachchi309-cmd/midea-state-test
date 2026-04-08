package lk.rumex.rumex_ott_mediaStat.mediaStatistics.exception;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res.ConcurrentStreamConflictResDTO;

public class ConcurrentStreamLimitExceededException extends RuntimeException {

    private final ConcurrentStreamConflictResDTO response;

    public ConcurrentStreamLimitExceededException(ConcurrentStreamConflictResDTO response) {
        super(response.message());
        this.response = response;
    }

    public ConcurrentStreamConflictResDTO getResponse() {
        return response;
    }
}
