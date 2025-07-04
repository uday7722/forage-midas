package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.Incentive;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private RestTemplate restTemplate;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {

                sender.setBalance(sender.getBalance() - transaction.getAmount());

                Incentive incentive = restTemplate.postForObject(
                        "http://localhost:8080/incentive",
                        transaction,
                        Incentive.class
                );

                float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0;

                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord record = new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                );
                transactionRepository.save(record);

                System.out.println("Transaction saved: " + transaction.getAmount() + " with incentive " + incentiveAmount);
            } else {
                System.out.println("Transaction discarded - insufficient funds.");
            }
        } else {
            System.out.println("Transaction discarded - invalid user.");
        }

        printWilburBalance();
    }

    private void printWilburBalance() {
        Iterable<UserRecord> users = userRepository.findAll();
        for (UserRecord user : users) {
            if ("wilbur".equalsIgnoreCase(user.getName())) {
                System.out.println("Wilbur's current balance = " + user.getBalance());
            }
        }
    }
}
