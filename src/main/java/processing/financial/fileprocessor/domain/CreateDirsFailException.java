package processing.financial.fileprocessor.domain;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateDirsFailException extends Exception {
    public CreateDirsFailException(String errorMessage) {
        super(errorMessage);
    }
}

