
public class Driver {
	public static void main(String[] args) throws Exception {
		/*
            args0: original dataset
            args1: output directory for DividerByUser job
            args2: output directory for coOccurrenceMatrixBuilder job
            args3: output directory for NormalizeCoOccurrenceMatrix job
            args4: output directory for MultiplicationMapperJoin job
            args5: output directory for MultiplicationSum job
            args6: output directory for RecommenderListGenerator job
        */
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		RecommanderListGenerator recommanderListGenerator = new RecommanderListGenerator();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String generator = args[6];

		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {normalizeDir, rawInput, multiplicationDir};
		String[] path5 = {multiplicationDir, sumDir};
		String[] path6 = {sumDir, generator};
		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		multiplication.main(path4);
		sum.main(path5);
		recommanderListGenerator.main(path6);

	}

}
