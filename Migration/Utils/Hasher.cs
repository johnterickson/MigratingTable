// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

namespace Migration
{
    // We want to make this a struct, so we have to live with people being
    // able to create default instances.
    public struct Hasher
    {
        readonly int x;
        public Hasher(int x)
        {
            this.x = x;
        }
        public Hasher With(int y)
        {
            unchecked
            {
                return new Hasher(31 * x + y);
            }
        }
        // With(object o)?  But the caller might want to use a custom EqualityComparer.
        // I'd rather pay the boilerplate in the caller than try to deal with that here.
        public static implicit operator int (Hasher h)
        {
            // If we cared about good distribution (e.g., different hashes
            // for dictionaries that differ by a permutation of the values),
            // we'd apply some function to h.x here.
            return h.x;
        }

        public static readonly Hasher Start = new Hasher(17);
    }
}
