package processing.financial.fileprocessor.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@NoArgsConstructor
@Getter
@Setter
@ToString
public class Transaction {
    //file item as a Transaction
    String accountNumber;
    BigDecimal amount;
}
