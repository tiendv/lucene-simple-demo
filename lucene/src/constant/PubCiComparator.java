/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package constant;

import dto.PubCiDTO;
import java.util.Comparator;

/**
 *
 * @author Huy Dang
 */
public class PubCiComparator implements Comparator<PubCiDTO> {

    @Override
    public int compare(PubCiDTO o1, PubCiDTO o2) {
        int age1 = o1.getYear();
        int age2 = o2.getYear();
        if (age1 > age2) {
            return 1;
        } else if (age1 == age2) {
            return 0;
        } else {
            return -1;
        }
    }
}