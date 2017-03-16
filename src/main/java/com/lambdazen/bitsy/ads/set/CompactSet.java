package com.lambdazen.bitsy.ads.set;

public class CompactSet {
    public static int size(Object set) {
        if (set == null) {
            return 0;
        } else if (set instanceof Set) {
            return ((Set)set).size();
        } else {
            return 1;
        }
    }

    public static Object[] getElements(Object set) {
        if (set == null) {
            return new Object[0];
        } else if (set instanceof Set) {
            return ((Set<?>)set).getElements();
        } else {
            return new Object[] {set};
        }
    }

    public static <T> Object add(Object set, T elem) {
        if (set == null) {
            return elem;
        } else if (set instanceof Set) {
            return ((Set)set).addElement(elem);
        } else {
            if (set.equals(elem)) {
                return set;
            } else {
                return new Set2(set, elem);
            }
        }
    }

    public static <T> Object addSafe(Object set, T elem) {
        if ((set instanceof Set24) && (CompactSet.size(set) == 24)) {
            // Move to ArraySet instead of SetMax to avoid cyclic dependency from CompactMultiSetMax and SetMax
            set = new ArraySet<T>(CompactSet.getElements(set));
        }
        
        return CompactSet.<T>add(set, elem);
    }
    
    public static <T> Object remove(Object set, T elem) {
        if (set == null) {
            return null;
        } else if (set instanceof Set) {
            return ((Set)set).removeElement(elem);
        } else {
            if ((set == elem) || (set.equals(elem))) {
                return null;
            } else {
                return set;
            }
        }
    }
}
