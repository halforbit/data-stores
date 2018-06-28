using System;

namespace Halforbit.DataStores.Exceptions
{
    public class ValidationError
    {
        public ValidationError(string message)
        {
            Message = message;
        }

        public ValidationError(string messageFormat, params object[] arguments)
        {
            Message = string.Format(messageFormat, arguments);
        }

        public string Message { get; private set; }

        public override string ToString() => Message;

        public static implicit operator ValidationError(string message) => new ValidationError(message);

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
