public class Movie implements Comparable<Movie>{
    private int movieId;
    private double rating;

    public Movie(int movieId, double rating) {
        this.movieId = movieId;
        this.rating = rating;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public int compareTo(Movie o) {
        double different = rating - o.getRating();
        if (different < 0) {
            return -1;
        }
        if (different > 0) {
            return 1;
        }
        return 0;
    }
}
