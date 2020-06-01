using System;

namespace Halforbit.DataStores
{
    public class ValidationError
    {
        public ValidationError(
            string message, 
            bool isFatal = false)
        {
            Message = message;

            IsFatal = isFatal;
        }

        public string Message { get; }

        public bool IsFatal { get; }

        public override string ToString() => Message;

        public static implicit operator ValidationError(string message) => new ValidationError(message);

        public static string AlreadyExists(string name)
        {
            return $"{name} already exists.";
        }

        public static string Required(string name)
        {
            return $"{name} is required.";
        }

        public static string DoesNotExist(string name, Guid id)
        {
            return $"{name} with id {id:N} does not exist.";
        }

        public static string DoesNotMatch(string nameA, string nameB, Guid value)
        {
            return $"{nameA} of {value:N} does not match {nameB}";
        }
    }
}
