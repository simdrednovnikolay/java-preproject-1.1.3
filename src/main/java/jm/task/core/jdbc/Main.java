package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {


    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();

        userService.createUsersTable();
        userService.saveUser("Alex", "Titov", (byte) 34);
        userService.saveUser("Svetlana","Tupareva",(byte) 23);
        userService.saveUser("Sergey","Egorov",(byte) 31);
        userService.saveUser("Yana","Vasileva",(byte) 43);
        userService.getAllUsers();
        userService.cleanUsersTable();
        userService.dropUsersTable();

    }


}
