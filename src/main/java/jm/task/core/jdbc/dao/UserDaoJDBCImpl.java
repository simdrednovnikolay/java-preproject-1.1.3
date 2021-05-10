package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;


public class UserDaoJDBCImpl extends Util implements UserDao {

    private final Connection connection = getConnection();
    private final Logger log = Logger.getLogger(UserDaoJDBCImpl.class.getName());

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() throws SQLException {
        Statement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE `user_connect`.`users` (\n" +
                    "  `id` INT NOT NULL AUTO_INCREMENT,\n" +
                    "  `name` VARCHAR(45) NULL,\n" +
                    "  `lastName` VARCHAR(45) NULL,\n" +
                    "  `age` INT NULL,\n" +
                    "  PRIMARY KEY (`id`));");
            connection.commit();
        } catch (SQLException t) {
            connection.rollback();
            t.printStackTrace();

        }

    }

    public void dropUsersTable() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate("DROP TABLE user_connect.users");
            connection.commit();

        } catch (SQLException t) {
            log.info("Таблица уже удалена!");
            connection.rollback();
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO user_connect.users" +
                " (NAME, LASTNAME,AGE) VALUES (?,?,?) ")) {
            connection.setAutoCommit(false);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);

            preparedStatement.executeUpdate();
            log.info("User " + " " + name + " " + "добавлен в базу!");
            connection.commit();

        } catch (SQLException t) {
            t.printStackTrace();
            connection.rollback();
        }
    }

    public void removeUserById(long id) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM user_connect.users " +
                "WHERE ID=? ")) {
            connection.setAutoCommit(false);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
            connection.close();
        } catch (SQLException t) {
            t.printStackTrace();
            connection.rollback();
        }
    }

    public List<User> getAllUsers() throws SQLException {
        List<User> allUsers = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            ResultSet resultSet = statement.executeQuery("SELECT ID,NAME,LASTNAME,AGE FROM user_connect.users");
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("ID"));
                user.setName(resultSet.getString("NAME"));
                user.setLastName(resultSet.getString("LASTNAME"));
                user.setAge(resultSet.getByte("AGE"));
                allUsers.add(user);
            }
            resultSet.close();
            connection.commit();

        } catch (SQLException t) {
            t.printStackTrace();
            connection.rollback();
        }
        System.out.print(allUsers.toString());
        return allUsers;

    }

    public void cleanUsersTable() throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("DELETE FROM user_connect.users")) {
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException t) {
            t.printStackTrace();
            connection.rollback();
        }

    }
}
