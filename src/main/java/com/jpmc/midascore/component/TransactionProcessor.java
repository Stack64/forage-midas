package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessor.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;

    public TransactionProcessor(UserRepository userRepository, TransactionRepository transactionRepository, IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void processTransaction(Transaction transaction) {
        logger.debug("Received transaction: {}", transaction);

        // Validate senderId
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            logger.debug("Transaction discarded: Invalid senderId {}", transaction.getSenderId());
            return;
        }

        // Validate recipientId
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            logger.debug("Transaction discarded: Invalid recipientId {}", transaction.getRecipientId());
            return;
        }

        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.debug("Transaction discarded: Insufficient balance. Sender balance: {}, Transaction amount: {}", 
                    sender.getBalance(), transaction.getAmount());
            return;
        }

        // All validations passed - process the transaction
        // Get incentive from Incentive API
        Incentive incentive = incentiveService.getIncentive(transaction);
        float incentiveAmount = incentive.getAmount();

        // Update sender balance (deduct transaction amount only, not incentive)
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        userRepository.save(sender);

        // Update recipient balance (add transaction amount + incentive)
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        userRepository.save(recipient);

        // Save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(transactionRecord);

        logger.debug("Transaction processed successfully: {}, incentive: {}", transaction, incentiveAmount);
    }
}
