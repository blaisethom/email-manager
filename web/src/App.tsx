import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import CompaniesPage from './pages/Companies';
import CompanyDetailPage from './pages/CompanyDetail';
import OrganizationDetailPage from './pages/OrganizationDetail';
import ContactsPage from './pages/Contacts';
import ContactDetailPage from './pages/ContactDetail';
import PersonDetailPage from './pages/PersonDetail';
import DiscussionsPage from './pages/Discussions';
import DiscussionDetailPage from './pages/DiscussionDetail';
import ActionsPage from './pages/Actions';
import CalendarEventsPage from './pages/CalendarEvents';
import JobsPage from './pages/Jobs';
import JobDetailPage from './pages/JobDetail';
import SearchPage from './pages/Search';
import TasksPage from './pages/Tasks';
import ReviewPage from './pages/Review';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/companies" replace />} />
          <Route path="companies" element={<CompaniesPage />} />
          <Route path="companies/:id" element={<CompanyDetailPage />} />
          <Route path="organizations/:id" element={<OrganizationDetailPage />} />
          <Route path="contacts" element={<ContactsPage />} />
          <Route path="contacts/:email" element={<ContactDetailPage />} />
          <Route path="people/:id" element={<PersonDetailPage />} />
          <Route path="discussions" element={<DiscussionsPage />} />
          <Route path="discussions/:id" element={<DiscussionDetailPage />} />
          <Route path="actions" element={<ActionsPage />} />
          <Route path="calendar" element={<CalendarEventsPage />} />
          <Route path="search" element={<SearchPage />} />
          <Route path="jobs" element={<JobsPage />} />
          <Route path="jobs/:id" element={<JobDetailPage />} />
          <Route path="tasks" element={<TasksPage />} />
          <Route path="review" element={<ReviewPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
