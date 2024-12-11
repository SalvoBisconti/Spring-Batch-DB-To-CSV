package com.advancia.stage.start;

import org.springframework.batch.item.ItemProcessor;

import com.advancia.stage.model.User;

public class UserItemProcessor implements ItemProcessor<User, User> {

 @Override
 public User process(User user) throws Exception {
  return user;
 }

}
