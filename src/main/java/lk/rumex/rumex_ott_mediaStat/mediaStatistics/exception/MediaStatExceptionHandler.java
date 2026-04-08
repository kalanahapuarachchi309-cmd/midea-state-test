package lk.rumex.rumex_ott_mediaStat.mediaStatistics.exception;

import org.springframework.http.HttpStatus;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res.ConcurrentStreamConflictResDTO;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class MediaStatExceptionHandler {

    @ExceptionHandler(ConcurrentStreamLimitExceededException.class)
    public ResponseEntity<ConcurrentStreamConflictResDTO> handleConcurrentStreamLimitExceeded(
            ConcurrentStreamLimitExceededException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getResponse());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleMethodArgumentNotValid(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                .toList();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", "VALIDATION_FAILED");
        body.put("message", "Request validation failed");
        body.put("errors", errors);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }
}
