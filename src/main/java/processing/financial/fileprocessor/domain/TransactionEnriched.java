package processing.financial.fileprocessor.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TransactionEnriched {
    UUID recordUniqueID;
    String batchID;
    String accountNumber;
    String itemStatus;
    String payload;
}
