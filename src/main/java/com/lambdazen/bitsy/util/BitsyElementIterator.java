package com.lambdazen.bitsy.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;

import com.lambdazen.bitsy.BitsyElement;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.UUID;

public abstract class BitsyElementIterator<BeanType, ElementType extends Element> implements Iterator<ElementType> {
    private Iterator<ElementType> changedElemIter;
    private Iterator<BeanType> elementIter;
    private HashSet<UUID> changedIds;
    private ElementType readAhead;
    private HashSet<UUID> allChangedIds;
    
    public BitsyElementIterator(Collection<BeanType> vertices, Iterator<ElementType> changedElemIter) {
        this.elementIter = vertices.iterator();
        this.changedIds = new HashSet<UUID>();
        this.readAhead = null;
        this.changedElemIter = changedElemIter;
    }
    
    public BitsyElementIterator(Collection<BeanType> vertices, Iterator<ElementType> changedElemIter, Collection<ElementType> allChangedVertices) {
        this.elementIter = vertices.iterator();
        this.changedIds = null;
        this.readAhead = null;
        this.changedElemIter = changedElemIter;
        this.allChangedIds = new HashSet<UUID>();
        for (Element elem : allChangedVertices) {
            allChangedIds.add((UUID)elem.id());
        }
    }

    public abstract UUID getId(BeanType bean);
    public abstract ElementType getElement(BeanType bean);
    
    public boolean hasNext() {
        // First return the changed vertices
        while ((readAhead == null) && (changedElemIter.hasNext())) {
            ElementType elem = changedElemIter.next();
            
            // Don't return this ID again
            if (changedIds != null) {
                changedIds.add((UUID)elem.id());
            }
            
            // Skip over deleted vertices
            if (((BitsyElement)elem).getState() != BitsyState.D) {
                // Found a good element
                readAhead = elem;
                break;
            }
        }
        
        // Found it?
        if (readAhead != null) return true;
        
        // Otherwise make sure that the next available vertex is not one of the changed IDs
        while ((readAhead == null) && (elementIter.hasNext())) {
            BeanType bean = elementIter.next();
            if (changedIds != null) {
                if (!changedIds.contains(getId(bean))) {
                    // A new vertex
                    readAhead = getElement(bean);
                }
            } else {
                // For indexes, we can't be sure what the diffs are. So any changed ID must be ignored.
                if (!allChangedIds.contains(getId(bean))) {
                //if ((changedIds == null) || !changedIds.contains(getId(bean))) {
                    // A new vertex
                    readAhead = getElement(bean);
                }
            }
        }
        
        return (readAhead != null);
    }

    public ElementType next() {
        if (readAhead == null) {
            // Running hasNext() in case the caller did not call it 
            hasNext();
        }
        
        ElementType ans = readAhead;
        if (ans == null) {
            // Still couldn't find it
            throw new NoSuchElementException();
        } else {
            // Go back and get the correct version from the transaction
            readAhead = null;
            return ans;
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
