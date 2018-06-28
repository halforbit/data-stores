using System;
using System.Collections.Generic;
using System.Linq;

namespace Halforbit.DataStores.Exceptions
{
    public class ValidationException : Exception
    {
        public ValidationException(
            IEnumerable<ValidationError> validationErrors)
        {
            ValidationErrors = validationErrors?.ToList().AsReadOnly() ??
                new List<ValidationError>().AsReadOnly();
        }

        public override string Message
        {
            get
            {
                return "Validation error(s) occurred: " +
                    string.Join("\r\n", ValidationErrors.Select(e => e.Message).ToArray());
            }
        }

        public IReadOnlyList<ValidationError> ValidationErrors { get; }
    }
}
