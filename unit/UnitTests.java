package unit;

import test.Series;
import test.SeriesReport;
import test.Test;

/** Runs all unit tests on distributed filesystem components.

    <p>
    There are currently no unit tests.
 */
public class UnitTests
{
    /** Runs the tests.

        @param arguments Ignored.
     */
    public static void main(String[] arguments)
    {
        // Create the test list, the series object, and run the test series.
        @SuppressWarnings("unchecked")
        Class<? extends Test>[]     tests =
            new Class[] {
                    conformance.common.PathTest.class,
                    conformance.naming.ContactTest.class,
                    conformance.naming.ListingTest.class,
                    conformance.naming.CreationTest.class,
                    conformance.naming.RegistrationTest.class,
                    conformance.rmi.StubTest.class,
                    conformance.rmi.SkeletonTest.class,
                    conformance.rmi.ThreadTest.class,
                    conformance.common.PathTest.class,
                    conformance.rmi.SkeletonTest.class,
                    conformance.rmi.StubTest.class,
                    conformance.rmi.ConnectionTest.class,
                    conformance.rmi.ThreadTest.class,
            };
        Series                      series = new Series(tests);
        SeriesReport                report = series.run(3, System.out);

        // Print the report and exit with an appropriate exit status.
        report.print(System.out);
        System.exit(report.successful() ? 0 : 2);
    }
}
