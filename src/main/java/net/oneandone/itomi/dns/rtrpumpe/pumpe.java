/* $Id: $
 */
package net.oneandone.itomi.dns.rtrpumpe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.xbill.DNS.Name;

/**
 *
 * @author miesi
 */
public class pumpe {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // evaluate ixfr pumpe
        // read config file
        CommandLine commandLine = null;
        Option option_s = Option.builder("s")
                .required(true)
                .desc("source zone name")
                .longOpt("source-zone")
                .build();
        Option option_d = Option.builder("d")
                .required(true)
                .desc("destination zone name")
                .longOpt("dst-zone")
                .build();
        Option option_H = Option.builder("H")
                .required(true)
                .desc("DNS Server for source Zone")
                .longOpt("source-dns-server")
                .build();
        Option option_r = Option.builder("r")
                .required(false)
                .desc("retransfer every n seconds (default 600)")
                .longOpt("retransfer")
                .type(Integer.class)
                .build();
        Option option_p = Option.builder("p")
                .required(false)
                .desc("DB properties (default /etc/rtrPumpeDB.properties")
                .longOpt("db-properties")
                .build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(option_s);
        options.addOption(option_d);
        options.addOption(option_H);
        options.addOption(option_r);
        options.addOption(option_p);

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Failed to parse commandline: " + e.toString());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("rtrPumpe", options);
            System.exit(1);
        }

        try {

            System.err.println("s: " + commandLine.getOptionValue('s'));
            System.err.println("d: " + commandLine.getOptionValue('d'));
            Thread t = new Thread(new PumpThread(new Name(commandLine.getOptionValue("s")), new Name(option_d.getValue()), option_H.getValue(), Integer.parseInt(option_r.getValue("600")), option_p.getValue()));
            t.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Fail: " + e.toString());
        }
    }
}
