package processing.financial.fileprocessor.domain;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LineLengthException extends Exception {
    public LineLengthException(String errorMessage) {
        super(errorMessage);
    }
}
