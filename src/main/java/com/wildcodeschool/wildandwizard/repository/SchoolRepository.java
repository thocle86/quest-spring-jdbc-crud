package com.wildcodeschool.wildandwizard.repository;

import com.wildcodeschool.wildandwizard.entity.School;
import com.wildcodeschool.wildandwizard.util.JdbcUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SchoolRepository implements CrudDao<School> {

    private final static String DB_URL = "jdbc:mysql://localhost:3306/spring_jdbc_quest?serverTimezone=GMT";
    private final static String DB_USER = "h4rryp0tt3r";
    private final static String DB_PASSWORD = "Horcrux4life!";

    @Override
    public School save(School school) {
        Connection connection = null;
		PreparedStatement statement = null;
		ResultSet generatedKeys = null;
        try {
            connection = DriverManager.getConnection(
            		DB_URL, DB_USER, DB_PASSWORD
            );
            statement = connection.prepareStatement(
            		"INSERT INTO school (name, capacity, country) VALUES (?, ?, ?)",
            		Statement.RETURN_GENERATED_KEYS
            );
            statement.setString(1, school.getName());
            statement.setLong(2, school.getCapacity());
            statement.setString(3, school.getCountry());

            if (statement.executeUpdate() != 1) {
                throw new SQLException("failed to insert data");
            }

            generatedKeys = statement.getGeneratedKeys();

            if (generatedKeys.next()) {
                Long id = generatedKeys.getLong(1);
                school.setId(id);
                return school;
            } else {
                throw new SQLException("failed to get inserted id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
		} finally {
			JdbcUtils.closeResultSet(generatedKeys);
			JdbcUtils.closeStatement(statement);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public School findById(Long id) {
        Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(
                    DB_URL, DB_USER, DB_PASSWORD
            );
            statement = connection.prepareStatement(
                    "SELECT * FROM school WHERE id=?;"
            );
            statement.setLong(1, id);
            resultSet = statement.executeQuery();

            if (resultSet.next()) {
                String name = resultSet.getString("name");
                Long capacity = resultSet.getLong("capacity");
                String country = resultSet.getString("country");
                return new School(id, name, capacity, country);
            }
        } catch (SQLException e) {
            e.printStackTrace();
		} finally {
			JdbcUtils.closeResultSet(resultSet);
			JdbcUtils.closeStatement(statement);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public List<School> findAll() {
        Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(
                    DB_URL, DB_USER, DB_PASSWORD
            );
            statement = connection.prepareStatement(
                    "SELECT * FROM school;"
            );
            resultSet = statement.executeQuery();

            List<School> schoolList = new ArrayList<>();

            while (resultSet.next()) {
                Long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                Long capacity = resultSet.getLong("capacity");
                String country = resultSet.getString("country");
                schoolList.add(new School(id, name, capacity, country));
            }
            return schoolList;
        } catch (SQLException e) {
            e.printStackTrace();
		} finally {
			JdbcUtils.closeResultSet(resultSet);
			JdbcUtils.closeStatement(statement);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public School update(School school) {
        Connection connection = null;
		PreparedStatement statement = null;
        try {
            connection = DriverManager.getConnection(
                    DB_URL, DB_USER, DB_PASSWORD
            );
            statement = connection.prepareStatement(
                    "UPDATE school SET name=?, capacity=?, country=? WHERE id=?"
            );
            statement.setString(1, school.getName());
            statement.setLong(2, school.getCapacity());
            statement.setString(3, school.getCountry());
            statement.setLong(4, school.getId());

            if (statement.executeUpdate() != 1) {
                throw new SQLException("failed to update data");
            }
            return school;
        } catch (SQLException e) {
            e.printStackTrace();
        }
		finally {
			JdbcUtils.closeStatement(statement);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public void deleteById(Long id) {
        Connection connection = null;
		PreparedStatement statement = null;
        try {
            connection = DriverManager.getConnection(
                    DB_URL, DB_USER, DB_PASSWORD
            );
            statement = connection.prepareStatement(
                    "DELETE FROM school WHERE id=?"
            );
            statement.setLong(1, id);

            if (statement.executeUpdate() != 1) {
                throw new SQLException("failed to delete data");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
		finally {
			JdbcUtils.closeStatement(statement);
			JdbcUtils.closeConnection(connection);
		}
    }
}
