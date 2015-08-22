// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using System.Collections.Generic;
using System.Linq;

namespace Migration
{
    public static class CollectionStatics
    {

        /* Learning a few C# concepts!  I'd like an analogue of Java's Map.get.  This only works if TValue
           is nullable (i.e., TValue : class or T? itself as an edge case).  If TValue is not nullable,
           (i.e., TValue : struct), I want the return type to be TValue? instead.  These two forms can't be
           overloaded without a weird trick:
           http://stackoverflow.com/questions/7764911/generic-contraints-on-method-overloads
           If we use different names, then from one POV, we may as well generalize the return-TValue case
           to GetValueOrDefault with no constraint.  I personally don't think the default value of a
           non-nullable type is useful, but others might.
           
           Note, "TValue : class" _can_ overload with "TValue = T?".  It's reasonable for these to be
           different overloads. */
        public static TValue GetValueOrDefault<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dict, TKey key)
        {
            TValue value;
            dict.TryGetValue(key, out value);
            return value;
        }
        public static TValue? GetValueNullable<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dict, TKey key) where TValue : struct
        {
            TValue value;
            if (dict.TryGetValue(key, out value))
                return value;
            else
                return null;
        }
        public static TValue GetValueOr<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dict, TKey key, TValue defaultValue)
        {
            TValue value;
            if (dict.TryGetValue(key, out value))
                return value;
            else
                return defaultValue;
        }
        // IDictionary does not extend IReadOnlyDictionary, so if we want to
        // support IDictionary (e.g., as used by ITableEntity) without making
        // Dictionary ambiguous, we would need two more overloads.

        // XXX: This algorithm is slow.
        public static IEnumerable<T> IntersectAll<T>(this IEnumerable<IEnumerable<T>> sets)
        {
            IEnumerable<T> partialIntersection = null;
            foreach (IEnumerable<T> set in sets)
            {
                partialIntersection = (partialIntersection == null) ? set : partialIntersection.Intersect(set);
            }
            if (partialIntersection == null)
                throw new ArgumentException("Cannot IntersectAll on an empty family of sets");
            return partialIntersection;
        }
    }
}
