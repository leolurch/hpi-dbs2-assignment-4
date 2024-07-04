import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

    Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Override
    public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
        Connection conn = null;
        Properties props = new Properties();
        props.put("user", config.getUsername());
        props.put("password", config.getPassword());

        conn = DriverManager.getConnection(
                "jdbc:postgresql://" +
                        config.getHost() +
                        ":" + config.getPort() +
                        "/" + config.getDatabase(),
                props);

        return conn;
    }

    @Override
    public List<Movie> queryMovies(
            @NotNull Connection connection,
            @NotNull String keywords
    ) throws SQLException {
        logger.info(keywords);
        List<Movie> movies = new ArrayList<>();

        String moviesQuery = "SELECT * FROM public.tmovies WHERE \"primaryTitle\" LIKE ? ORDER BY \"primaryTitle\" ASC, \"startYear\" ASC";
        String actorsQuery = "SELECT * FROM public.tprincipals JOIN public.nbasics ON public.nbasics.nconst = public.tprincipals.nconst WHERE \"tconst\" = ? AND \"category\" IN ('actor','actress') ORDER BY public.nbasics.primaryname ASC";

        PreparedStatement moviesStatement = connection.prepareStatement(moviesQuery);
        moviesStatement.setString(1, "%" + keywords + "%");

        ResultSet resultSet = moviesStatement.executeQuery();

        while (resultSet.next()) {
            Array sqlArray = resultSet.getArray("genres");
            String[] javaArray = (String[]) sqlArray.getArray();
            Set<String> genresSet = new HashSet<>(Arrays.asList(javaArray));

            var myMovie = new Movie(
                    resultSet.getString("tconst"),
                    resultSet.getString("primaryTitle"),
                    resultSet.getInt("startYear"),
                    genresSet
            );

            PreparedStatement actorsStatement = connection.prepareStatement(actorsQuery);
            actorsStatement.setString(1, myMovie.tConst);

            ResultSet actorsResultSet = actorsStatement.executeQuery();
            while (actorsResultSet.next()) {
                myMovie.actorNames.add(actorsResultSet.getString("primaryname"));
            }

            movies.add(myMovie);
        }

        return movies;
    }
    @Override
    public List<Actor> queryActors(
            @NotNull Connection connection,
            @NotNull String keywords
    ) throws SQLException {
        List<Actor> actors = new ArrayList<>();

        // Query to find top 5 actors with the most movies matching the keyword
        String topActorsQuery = """
        SELECT a."nconst", a."primaryname"
        FROM public."nbasics" a
        JOIN public."tprincipals" p ON a."nconst" = p."nconst"
        WHERE p."category" IN ('actor', 'actress')
          AND a."primaryname" LIKE ?
        GROUP BY a."nconst", a."primaryname"
        ORDER BY COUNT(p."tconst") DESC, a."primaryname" ASC
        LIMIT 5
    """;

        PreparedStatement topActorsStmt = connection.prepareStatement(topActorsQuery);
        topActorsStmt.setString(1, "%" + keywords + "%");
        ResultSet topActorsRs = topActorsStmt.executeQuery();

        while (topActorsRs.next()) {
            String actorNConst = topActorsRs.getString("nconst");
            String actorName = topActorsRs.getString("primaryname");

            Actor actor = new Actor(actorNConst, actorName);

            // Query for top 5 latest movies for the actor
            String latestMoviesQuery = """
            SELECT DISTINCT m."primaryTitle", m."startYear"
            FROM public."tmovies" m
            JOIN public."tprincipals" p ON m."tconst" = p."tconst"
            WHERE p."nconst" = ?
            ORDER BY m."startYear" DESC, m."primaryTitle" ASC
            LIMIT 5
        """;

            PreparedStatement latestMoviesStmt = connection.prepareStatement(latestMoviesQuery);
            latestMoviesStmt.setString(1, actorNConst);
            ResultSet latestMoviesRs = latestMoviesStmt.executeQuery();

            while (latestMoviesRs.next()) {
                actor.playedIn.add(latestMoviesRs.getString("primaryTitle"));
            }

            // Query for top 5 co-actors
            String coActorsQuery = """
            SELECT co."primaryname", COUNT(*) AS count
            FROM public."tprincipals" p1
            JOIN public."tprincipals" p2 ON p1."tconst" = p2."tconst"
            JOIN public."nbasics" co ON p2."nconst" = co."nconst"
            WHERE p1."nconst" = ? AND p2."nconst" <> p1."nconst"
              AND p2."category" IN ('actor', 'actress')
            GROUP BY co."primaryname"
            ORDER BY count DESC, co."primaryname" ASC
            LIMIT 5
        """;

            PreparedStatement coActorsStmt = connection.prepareStatement(coActorsQuery);
            coActorsStmt.setString(1, actorNConst);
            ResultSet coActorsRs = coActorsStmt.executeQuery();

            while (coActorsRs.next()) {
                actor.costarNameToCount.put(coActorsRs.getString("primaryname"), coActorsRs.getInt("count"));
            }

            actors.add(actor);
        }

        // Sorting the list of actors
        actors.sort(
                Comparator.comparingInt((Actor a) -> a.playedIn.size()).reversed()
                        .thenComparing(a -> a.name)
        );

        return actors;
    }

}
