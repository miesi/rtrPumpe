/* $Id: $
 */
package net.oneandone.itomi.dns.rtrpumpe;

import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.MXRecord;
import org.xbill.DNS.NSRecord;
import org.xbill.DNS.Name;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.Record;
import org.xbill.DNS.Type;

/**
 *
 * @author miesi
 */
public class PdnsDbRecord {

    private String name;
    private int type;
    private long ttl;
    private Integer prio;
    private String content;

    public PdnsDbRecord(String name, int type, long ttl, String content) {
        this.name = name;
        this.type = type;
        this.ttl = ttl;
        this.content = content;
    }

    public PdnsDbRecord(String name, String type, long ttl, String content) {
        this.name = name;
        this.type = Type.value(type);
        this.ttl = ttl;
        this.content = content;
    }

    public PdnsDbRecord(String name, int type, long ttl, Integer prio, String content) {
        this.name = name;
        this.type = type;
        this.ttl = ttl;
        this.prio = prio;
        this.content = content;
    }

    public PdnsDbRecord(String name, String type, long ttl, Integer prio, String content) {
        this.name = name;
        this.type = Type.value(type);
        this.ttl = ttl;
        this.prio = prio;
        this.content = content;
    }

    public PdnsDbRecord(Record r) throws Exception {
        this.name = r.getName().toString(true);
        this.type = r.getType();
        this.ttl = r.getTTL();
        switch (type) {
            case Type.MX:
                MXRecord mxr = (MXRecord) r;
                this.prio = mxr.getPriority();
                this.content = mxr.getTarget().toString(true);
                break;
            case Type.CNAME:
                CNAMERecord cr = (CNAMERecord) r;
                this.content = cr.getTarget().toString(true);
                break;
            case Type.NS:
                NSRecord nr = (NSRecord) r;
                this.content = nr.getTarget().toString(true);
                break;
            case Type.SRV:
                throw new Exception("Correct conversion for SRV Records not implemented");

            case Type.PTR:
                PTRRecord ptr = (PTRRecord) r;
                this.content = ptr.getTarget().toString(true);
                break;
            default:
                this.content = r.rdataToString();
        }

        // somehow remove trailing dots from content
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return Type.string(type);
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public Integer getPrio() {
        return prio;
    }

    public void setPrio(Integer prio) {
        this.prio = prio;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(" ");
        sb.append(ttl);
        sb.append(" ");
        sb.append(Type.string(type));
        sb.append(" ");
        sb.append(content);
        return sb.toString();
    }

    public void toNewZone(Name srcZoneName, Name dstZoneName) throws Exception {
        Name rel = new Name(name).relativize(srcZoneName);
        Name dstName = Name.concatenate(rel, dstZoneName);
        this.name = dstName.toString(true);
        // fix trailing dots in content

        // special CNAME Handling (transform x.y. cname y.y. to x.z. cname y.z.
        // special handling for @ NS Records?
        // special handling for primary NS in SOA?
    }
}
