package kz.ffloppa;

import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.sql.*;


public class Database {
    private static final Logger log = Loggers.getLogger(Database.class);

    private static final String URL = "jdbc:" + Main.dotenv.get("postgresql_url");
    private static final String USER = Main.dotenv.get("postgresql_user");
    private static final String PASSWORD = Main.dotenv.get("postgresql_password");
    private Statement statement;
    private Connection connection;

    Database() {
        try {
            this.connection = DriverManager.getConnection(URL, USER, PASSWORD);
            this.statement = connection.createStatement();
            log.info("[DB] Successfully connected!");
        } catch (SQLException e) {
            log.error("[DB] Connection Failed!", e);
        }
    }

    public void setData(String id, String nickname) throws SQLException {
        String sql = "INSERT INTO public.members (user_id, nickname) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, nickname);
            stmt.executeUpdate();

            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    public void removeId(String id) throws SQLException {
        String sql = "DELETE FROM public.members WHERE user_id = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.executeUpdate();

            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    public boolean isUserIdExists(String id) throws SQLException {
        String sql = "SELECT EXISTS(SELECT 1 FROM public.members WHERE user_id = ?)";
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

    public String getUserNickname(String id) throws SQLException {
        String sql = "SELECT nickname FROM public.members WHERE user_id = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("nickname");
                }
            }
        } catch (SQLException e) {
            log.error("Error executing SELECT query for user: " + id, e);
            throw e;
        }
        return null;
    }
}
