/* $Id:$
 */
package net.oneandone.itomi.dns.rtrpumpe;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.Type;
import org.xbill.DNS.ZoneTransferException;
import org.xbill.DNS.ZoneTransferIn;

/**
 *
 * @author miesi
 */
public class PumpThread implements Runnable {

    private volatile boolean keepOnRunning = true;
    private final Name srcZoneName;
    private final Name dstZoneName;
    private final String srcDnsServer;
    private final Integer pollInterval;
    private Connection cn;
    private List records;
    private Properties rtrPumpeProperties;
    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPass;
    private final String jdbcClass;

    private PumpThread() {
        // do stupid things to silence alarms
        srcZoneName = null;
        dstZoneName = null;
        srcDnsServer = null;
        pollInterval = null;
        jdbcUrl = null;
        dbUser = null;
        dbPass = null;
        jdbcClass = null;
    }

    public PumpThread(Name srcZoneName, Name dstZoneName, String srcDnsServer, Integer pollInterval, String propFileName) throws Exception {
        this.srcZoneName = srcZoneName;
        this.dstZoneName = dstZoneName;
        this.srcDnsServer = srcDnsServer;
        this.pollInterval = pollInterval;
        try {
            if (propFileName == null) {
                propFileName = "/etc/rtrPumpeDB.properties";
            }
            BufferedInputStream stream = new BufferedInputStream(new FileInputStream(propFileName));
            rtrPumpeProperties.load(stream);
            stream.close();
        } catch (IOException ex) {
            Logger.getLogger(PumpThread.class.getName()).log(Level.INFO, "Failed to load Database Properties File, using defaults", ex);
        }

        jdbcUrl = rtrPumpeProperties.getProperty("jdbcUrl", "jdbc:mysql://localhost:3306/pdns_pumped?useServerPrepStmts=true");
        dbUser = rtrPumpeProperties.getProperty("dbUser", "root");
        dbPass = rtrPumpeProperties.getProperty("dbPass", "");
        jdbcClass = rtrPumpeProperties.getProperty("jdbcClass", "com.mysql.jdbc.Driver");
    }

    @Override
    public void run() {
        int i = 0;
        while (keepOnRunning) {
            i++;
            try {
                try {
                    Class.forName(jdbcClass);
                    cn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
                    cn.setAutoCommit(false);
                    cn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                } catch (ClassNotFoundException | SQLException e) {
                    Logger.getLogger(PumpThread.class.getName()).log(Level.SEVERE, "Database connection failed", e);
                    throw new Exception("Database connection failed: " + e.toString());
                }

                try {
                    ZoneTransferIn xfr = ZoneTransferIn.newAXFR(srcZoneName, srcDnsServer, null);
                    records = xfr.run();
                } catch (IOException | ZoneTransferException e) {
                    Logger.getLogger(PumpThread.class.getName()).log(Level.SEVERE, "Zone transfer failed for: " + srcDnsServer + " " + srcZoneName, e);
                    throw new Exception("Zone transfer failed for: " + srcDnsServer + " " + srcZoneName + " Ex: " + e.toString());
                }
                // get domainid (and fail if it is not there)
                int domId;
                try {
                    PreparedStatement stSelDomId = cn.prepareStatement("select id from domains where name=?");
                    stSelDomId.setString(1, dstZoneName.toString(true));
                    ResultSet rsDomId = stSelDomId.executeQuery();
                    rsDomId.first();
                    domId = rsDomId.getInt(1);
                } catch (SQLException e) {
                    Logger.getLogger(PumpThread.class.getName()).log(Level.SEVERE, "Failed to find domain_id for " + dstZoneName, e);
                    throw new Exception("Failed to find domain_id for " + dstZoneName);
                }
                //  check if there is more in the result set?

                // remove all old records (silly simple implementation)
                PreparedStatement delDomId = cn.prepareStatement("delete from records where domain_id=?");
                delDomId.setInt(1, domId);
                delDomId.execute();

                // prepare insert statement
                PreparedStatement insRec = cn.prepareStatement("insert into records(domain_id, name, type, ttl, prio, content) values (" + domId + ",?,?,?,?,?)");
                boolean soaSet = false;
                for (Iterator it = records.iterator(); it.hasNext();) {

                    Record r = (Record) it.next();
                    // filter out DNSSec (RRSIG, DNSKEY, NSEC3PARAM and NSEC3 stuff)
                    if (r.getType() == Type.RRSIG
                            || r.getType() == Type.DS
                            || r.getType() == Type.DNSKEY
                            || r.getType() == Type.NSEC
                            || r.getType() == Type.NSEC3
                            || r.getType() == Type.NSEC3PARAM) {
                        continue;
                    }
                    // handle the duplicate SOA in axfr
                    if (soaSet == true && r.getType() == Type.SOA) {
                        continue;
                    }
                    if (soaSet == false && r.getType() == Type.SOA) {
                        soaSet = true;
                    }

                    PdnsDbRecord dbr = new PdnsDbRecord(r);
                    dbr.toNewZone(srcZoneName, dstZoneName);
                    insRec.setString(1, dbr.getName());
                    insRec.setString(2, dbr.getType());
                    insRec.setLong(3, dbr.getTtl());
                    if ("MX".equals(dbr.getType())) {
                        insRec.setInt(4, dbr.getPrio());
                    } else {
                        insRec.setNull(4, Types.INTEGER);
                    }
                    insRec.setString(5, dbr.getContent());
                    insRec.executeUpdate();
                }

                insRec.close();
                cn.commit();
                cn.close();
            } catch (Exception e) {
                Logger.getLogger(PumpThread.class.getName()).log(Level.SEVERE, null, e);

                try {
                    cn.rollback();
                } catch (SQLException ex) {
                    Logger.getLogger(PumpThread.class.getName()).log(Level.FINE, null, ex);
                }
                try {
                    cn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PumpThread.class.getName()).log(Level.FINE, null, ex);
                }

                if (i < 2) {
                    Logger.getLogger(PumpThread.class.getName()).log(Level.WARNING, "Serious Problem on first try, exiting");
                    System.exit(1);
                }
            }
            try {
                Thread.sleep(pollInterval * 1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(PumpThread.class.getName()).log(Level.FINER, "interrupted sleep", ex);
            }
        }
    }

    public void stopThread() {
        keepOnRunning = false;
    }
}
