package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);

        // Query waldorf's balance
        UserRecord waldorf = userRepository.findByName("waldorf");
        if (waldorf != null) {
            float balance = waldorf.getBalance();
            int balanceRoundedDown = (int) Math.floor(balance);
            logger.info("----------------------------------------------------------");
            logger.info("----------------------------------------------------------");
            logger.info("----------------------------------------------------------");
            logger.info("Waldorf's balance after all transactions: {}", balance);
            logger.info("Waldorf's balance (rounded down to nearest integer): {}", balanceRoundedDown);
            logger.info("----------------------------------------------------------");
        } else {
            logger.error("Waldorf user not found in database");
        }
    }
}
