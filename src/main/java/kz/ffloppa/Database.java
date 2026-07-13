package kz.ffloppa;

import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.sql.*;


public class Database {
    private static final Logger log = Loggers.getLogger(Database.class);

    private static final String URL = System.getenv("postgresql_url");
    private static final String USER = System.getenv("postgresql_user");
    private static final String PASSWORD = System.getenv("postgresql_password");
    private Statement statement;
    private Connection connection;

    Database() {
        Mono.fromSupplier(() -> {
            try (Connection connection1 = DriverManager.getConnection(URL, USER, PASSWORD)) {
                statement = connection1.createStatement();
                connection = connection1;
                log.debug("[DB] Successfully connected!");

                String query = "SELECT version();";
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    if (resultSet.next()) {
                        log.debug("[DB] Version: " + resultSet.getString(1));
                    }
                }

            } catch (SQLException e) {
                log.error("[DB] Connection Failed!" + "\n\n" + e.getMessage());
            }
            return null;
        }).subscribe(
                result -> {},
                error -> log.error("[DB] Connection Failed!" + "\n\n" + error.getMessage())
        );
    }

    public void setData(String id, String nickname) throws SQLException {
        statement.executeQuery("INSERT INTO public.members (user_id,nickname)\nVALUES(" + id + ", " + nickname.replace("SELECT", "") + ")");
    }

    public void removeId(String id) throws SQLException {
        statement.executeQuery("DELETE FROM public.members\nWHERE user_id = '" + id + "'");
    }

    public boolean isUserIdExists(String id) throws SQLException {
        String sql = "SELECT EXISTS(SELECT 1 FROM public.members WHERE user_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                // rs.next() перемещает указатель на следующую (в данном случае первую) строку.
                // Если она есть — метод вернет true.
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
            }
        } catch (SQLException e) {
            log.error("Ошибка при выполнении SELECT", e);
        }
        return false;
    }
}
